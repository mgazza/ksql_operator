/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"k8s.io/apimachinery/pkg/api/meta"
	"ksql_operator/ksqlclient"
	"ksql_operator/ksqlclient/swagger"
	"ksql_operator/ksqlparser"
	"strconv"
	"strings"
	"time"

	"github.com/go-test/deep"
	//errors2 "github.com/pkg/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	ksqloperatorv1alpha1 "ksql_operator/pkg/apis/ksql_operator/v1alpha1"

	clientset "ksql_operator/pkg/generated/clientset/versioned"
	mgazzaScheme "ksql_operator/pkg/generated/clientset/versioned/scheme"
	informers "ksql_operator/pkg/generated/informers/externalversions/ksql_operator/v1alpha1"
	listers "ksql_operator/pkg/generated/listers/ksql_operator/v1alpha1"
)

const controllerAgentName = "ksql-manager"

type KSQLClient interface {
	Describe(ctx context.Context, name string) (interface{}, error)
	Explain(ctx context.Context, name string) (interface{}, error)
	Execute(ctx context.Context, stmt string, result interface{}) (interface{}, error)
	CreateDropTerminate(ctx context.Context, stmt string) (interface{}, error)
	Status(tx context.Context, commandID string) (interface{}, error)
}

var DefaultHasher = func(s string) string {
	hasher := sha256.New()
	hasher.Write([]byte(s))
	return base64.URLEncoding.EncodeToString(hasher.Sum(nil))
}

// Controller is the controller implementation for KSQLDefinition resources
type Controller struct {
	// clientSet is a clientset for our own API group
	clientSet clientset.Interface
	// k8s API group
	kubeclientset kubernetes.Interface

	managedKSQLLister listers.ManagedKSQLLister
	ManagedKSQLSynced cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder

	// ksqlClient is an interface that allows us to connect to the kafka connect api
	ksqlClient KSQLClient

	//cache is a thread safe cache for storing previously seen resources
	cache *safeCache
}

// NewController returns a new sample controller
func NewController(
	kubeClientSet kubernetes.Interface,
	clientSet clientset.Interface,
	ksqlDefinitionInformer informers.ManagedKSQLInformer,
	ksqlClient KSQLClient,
) *Controller {

	// Create event broadcaster
	// Add sample-controller types to the default Kubernetes Scheme so Events can be
	// logged for our types.
	utilruntime.Must(mgazzaScheme.AddToScheme(mgazzaScheme.Scheme))
	klog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartStructuredLogging(0)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeClientSet.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(mgazzaScheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &Controller{
		kubeclientset:     kubeClientSet,
		clientSet:         clientSet,
		managedKSQLLister: ksqlDefinitionInformer.Lister(),
		ManagedKSQLSynced: ksqlDefinitionInformer.Informer().HasSynced,
		workqueue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "ManagedKSQLs"),
		recorder:          recorder,
		ksqlClient:        ksqlClient,
	}

	klog.Info("Setting up event handlers")
	// Set up an event handler for when KSQLDefinition resources change

	controller.cache = NewSafeCache()

	ksqlDefinitionInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: controller.enqueueManagedKSQL,
			UpdateFunc: func(old, new interface{}) {
				// diff and only queue on changes
				if diff := deep.Equal(old, new); diff != nil {
					controller.enqueueManagedKSQL(new)
				}
			},
			DeleteFunc: controller.enqueueManagedKSQL,
		})
	return controller
}

// GetNewerByResourceVersion takes a and b which are k8s resources and returns the one with the latest version number
func (c *Controller) GetNewerByResourceVersion(a, b interface{}) (interface{}, error) {
	metaA, err := meta.Accessor(a)
	if err != nil {
		return nil, fmt.Errorf("object a has no meta: %v", err)
	}
	metaB, err := meta.Accessor(b)
	if err != nil {
		return nil, fmt.Errorf("object b has no meta: %v", err)
	}

	verA, err := strconv.Atoi(metaA.GetResourceVersion())
	if err != nil {
		return nil, fmt.Errorf("unable to parse metadata.resourceVersion for object a : %v", err)
	}

	verB, err := strconv.Atoi(metaB.GetResourceVersion())
	if err != nil {
		return nil, fmt.Errorf("unable to parse metadata.resourceVersion for object b : %v", err)
	}

	if verA > verB {
		return a, nil
	}
	return b, nil
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	klog.Info("Starting KSQLDefinition controller")

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.ManagedKSQLSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	klog.Info("Starting workers")
	// Launch two workers to process KSQLDefinition resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	klog.Info("Started workers")
	<-stopCh
	klog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			utilruntime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// KSQLDefinition resource to be synced.
		if err := c.syncHandler(key); err != nil {
			// Put the item back on the workqueue to handle any transient errors.
			c.workqueue.AddRateLimited(key)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		klog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

func (c *Controller) splitMetaNamespaceKey(key string) (string, string, error) {
	return cache.SplitMetaNamespaceKey(key)
}

func (c *Controller) getKeyForResource(obj interface{}) (string, error) {
	return cache.MetaNamespaceKeyFunc(obj)
}

type cacheItem struct {
	Resource *ksqloperatorv1alpha1.ManagedKSQL
	Stmts    []ksqlparser.Stmt
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the KSQLDefinition resource
// with the current status of the resource.
func (c *Controller) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := c.splitMetaNamespaceKey(key)

	if err != nil {
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the KSQLDefinition resource with this namespace/name
	managedKSQL, err := c.managedKSQLLister.ManagedKSQLs(namespace).Get(name)
	if err != nil {
		// The KSQLDefinition resource may no longer exist if its been deleted
		if errors.IsNotFound(err) {
			klog.Infof("Cleaning up %s", key)
			item := c.cache.Get(key)
			if item == nil {
				// We've never seen this before :?
				klog.Warningf("resource '%s' has never been seen before by this controller no action will be taken", key)
			}
			ci, ok := item.(*cacheItem)
			if !ok {
				return fmt.Errorf("unexpected type %t in cache", item)
			}
			// for each of the stmts we need to delete them
			for _, v := range ci.Resource.Status.ItemStatus {
				if v.QueryID != "" {
					klog.V(5).Infof("terminating %s", v.QueryID)
					err := c.Terminate(v.QueryID)
					if err != nil {
						klog.Errorf("error terminating query '%s': %v", v.QueryID, err)
					}
				}
			}
			for _, stmt := range ci.Stmts {
				switch stmt.GetActionType() {
				case ksqlparser.StmtTypeCreate:
					fallthrough
				case ksqlparser.StmtTypeCreateOrReplace:
					createStmt := stmt.(ksqlparser.CreateStmt)
					t := createStmt.GetObjectType()
					klog.V(5).Infof("dropping %s %s", t, stmt.GetName())
					err := c.DropTableStreamChain(string(t), stmt.GetName())
					if err != nil {
						klog.Errorf("error dropping %s %s: %v", t, stmt.GetName(), err)
					}
				}
			}

			//finally
			c.cache.Delete(key)

			return nil
		}

		// If an error occurs during Get, we'll requeue the item so we can
		// attempt processing again later. This could have been caused by a
		// temporary network failure, or any other transient reason.
		return err
	}

	var stmts []ksqlparser.Stmt
	// pull or build stmts out of the cache by using closure
	err = c.cache.Sync(key, func(obj interface{}) (interface{}, error) {
		ci := &cacheItem{}
		if obj != nil {
			c, ok := obj.(*cacheItem)
			if !ok {
				return nil, fmt.Errorf("unexpected type %t in cache", obj)
			}
			ci = c
		}
		if ci.Resource == nil || ci.Resource.ResourceVersion < managedKSQL.ResourceVersion {
			klog.V(4).Info("parsing ksql")
			// lets parse the statement in this resource
			stmts, err = ksqlparser.Parse(managedKSQL.Statement)
			if err != nil {
				return nil, err

			}
			klog.V(4).Info("building dependency graph")
			//sort statements in a safe dependency order
			lenin := len(stmts)
			stmts = ksqlparser.BuildDependencyGraph(stmts)
			if lenin != len(stmts) {
				// some kind of dependency loop
				err := fmt.Errorf("error resolving dependencies - possible dependency loop")
				return nil, err
			}
			ci.Resource = managedKSQL
			ci.Stmts = stmts
		} else {
			stmts = ci.Stmts
		}
		return ci, nil
	})
	if err != nil {
		return fmt.Errorf("error pulling key from cache: %v", err)
	}

	if managedKSQL.Status.ItemStatus == nil {
		managedKSQL.Status.ItemStatus = map[string]ksqloperatorv1alpha1.CommandStatus{}
	}

	defer c.updateManagedKSQLStatus(managedKSQL)

	managedKSQL.Status.Applied = ksqloperatorv1alpha1.StatusPending

	//check status in stmt order by name
	stmtNames := map[string]ksqlparser.Stmt{}
	for _, stmt := range stmts {
		name := stmt.GetName()
		stmtNames[name] = stmt
		commandStatus := managedKSQL.Status.ItemStatus[name]

		err := c.processStmt(stmt, &commandStatus)
		managedKSQL.Status.ItemStatus[name] = commandStatus
		if err != nil {
			return err
		}
	}

	// find any which we are tracking which no longer exist in the status and drop/terminate them
	var dropped []string

	for k, v := range managedKSQL.Status.ItemStatus {
		if _, ok := stmtNames[k]; ok {
			continue
		}
		dropped = append(dropped, k)
		if v.CommandID == "" || v.QueryID == "" {
			klog.Warning(fmt.Sprintf("ignoring [%s] which does not form part of a current stmt and is missing a command/query identifier", k))
			// we have no reference to manage this
			continue
		}

		if v.CommandID != "" {
			t, n, err := c.parseTypeAndNameFromCommand(v)
			if err != nil {
				klog.Error(err)
				continue
			}
			err = c.DropTableStreamChain(t, n)
			if err != nil {
				return err
			}
		}
	}

	for _, d := range dropped {
		delete(managedKSQL.Status.ItemStatus, d)
	}

	managedKSQL.Status.Applied = ksqloperatorv1alpha1.StatusApplied

	return nil
}

func (c *Controller) parseTypeAndNameFromCommand(v ksqloperatorv1alpha1.CommandStatus) (string, string, error) {
	// command ids are of a format stream|table/'name'/etc
	commandParts := strings.Split(v.CommandID, "/")
	if len(commandParts) < 2 {
		return "", "", fmt.Errorf("command id '%s' was not in the expected format", v.CommandID)
	}
	t := commandParts[0]
	if len(commandParts[1]) < 2 {
		return "", "", fmt.Errorf("command id name part '%s' was not in the expected format", v.CommandID[1])
	}
	// the name is quoted so trim the quotes
	n := commandParts[1][1 : len(commandParts[1])-2]
	return t, n, nil
}

// enqueueManagedKSQL takes a KSQLDefinition resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than KSQLDefinition.
func (c *Controller) enqueueManagedKSQL(obj interface{}) {
	key, err := c.getKeyForResource(obj)
	if err != nil {
		utilruntime.HandleError(err)
	}
	c.workqueue.Add(key)
}

func (c *Controller) setManagedResourceStatus(ManagedKSQL *ksqloperatorv1alpha1.ManagedKSQL, status ksqloperatorv1alpha1.ResourceStatus) {
	cp := ManagedKSQL.DeepCopy()
	cp.Status.Applied = status
	err := c.updateManagedKSQLStatus(cp)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("error updating status: %v", err))
	}
}

func (c *Controller) updateManagedKSQLStatus(ManagedKSQL *ksqloperatorv1alpha1.ManagedKSQL) error {
	// If the CustomResourceSubResources feature gate is not enabled,
	// we must use Update instead of UpdateStatus to update the Status block of the KSQLDefinition resource.
	// UpdateStatus will not allow changes to the Spec of the resource,
	// which is ideal for ensuring nothing other than resource status has been updated.
	_, err := c.clientSet.MgazzaV1alpha1().ManagedKSQLs(ManagedKSQL.Namespace).
		UpdateStatus(context.Background(), ManagedKSQL, metav1.UpdateOptions{})
	return err
}

func (c *Controller) processStmt(stmt ksqlparser.Stmt, commandStatus *ksqloperatorv1alpha1.CommandStatus) error {
	klog.V(5).Infof("processing stmt '%s'", stmt.GetName())
	ksql := stmt.String()
	hash := DefaultHasher(ksql)
	switch stmt.GetActionType() {
	case ksqlparser.StmtTypeCreate:
		if commandStatus.CommandID == "" {
			// lets describe it to see if it already exists
			resp, err := c.ksqlClient.Describe(context.Background(), stmt.GetName())
			if err != nil {
				return err
			}
			switch resp.(type) {
			case *swagger.ModelError:
				modelErr := resp.(*swagger.ModelError)
				if modelErr.ErrorCode != ksqlclient.ErrCodeNotFound {
					err = fmt.Errorf("error response from ksql: (%0f) %s\n%s",
						modelErr.ErrorCode, modelErr.Message, strings.Join(modelErr.StackTrace, "\n"))
					return err
				}
			case *[]swagger.DescribeResultItem:
				modelResult := resp.(*[]swagger.DescribeResultItem)
				if len(*modelResult) != 1 {
					// this is likely to be unrecoverable
					err := fmt.Errorf("expected only one response from ksql but got %d, \n %v", len(*modelResult), modelResult)
					utilruntime.HandleError(err)
					return nil
				}
				stmtHash := DefaultHasher((*modelResult)[0].SourceDescription.Statement)
				commandStatus.StatusSha = stmtHash

				// set the stage so that the below drop code executes
				commandStatus.QuerySha = ""
			}
		}
		if commandStatus.QuerySha != hash {
			klog.V(5).Info("querySha differs issuing drop")
			commandStatus.CommandID = ""
			t := stmt.(ksqlparser.CreateStmt).GetObjectType()
			err := c.DropTableStreamChain(string(t), stmt.GetName())
			if err != nil && err != ErrNotFound {
				return err
			}
		}
		fallthrough
	case ksqlparser.StmtTypeCreateOrReplace:
		// do we need to create or update this?
		if commandStatus.CommandID == "" || commandStatus.QuerySha != hash {
			klog.V(5).Info("commandId is not set or querySha differs issuing create")

			if err := c.processCreateOrReplaceStmt(ksql, hash, commandStatus); err != nil {
				return err
			}
			return nil
		}
		// lets update the status
		cmdId := commandStatus.CommandID
		klog.V(5).Info("getting status")
		response, err := c.ksqlClient.Status(context.Background(), cmdId)
		if err != nil {
			return err
		}
		switch response.(type) {
		case *swagger.ModelError:
			modelErr := response.(*swagger.ModelError)
			if modelErr.ErrorCode == 404 {
				commandStatus.CommandID = ""
				return fmt.Errorf("command %s was not found clearing commandID", cmdId)
			}
		case *swagger.StatusResponse:
			resp := response.(*swagger.StatusResponse)
			stat, err := ksqloperatorv1alpha1.ParseCommandStatus(resp.Status)
			if err != nil {
				return err
			}
			commandStatus.Status = stat
		}
		// lets describe to get the stmt to check for differences
		klog.V(5).Info("describing stmt")
		resp, err := c.ksqlClient.Describe(context.Background(), stmt.GetName())
		if err != nil {
			return err
		}
		switch resp.(type) {
		case *swagger.ModelError:
			modelErr := resp.(*swagger.ModelError)
			err := fmt.Errorf("error response from ksql: (%0f) %s\n%s",
				modelErr.ErrorCode, modelErr.Message, strings.Join(modelErr.StackTrace, "\n"))
			utilruntime.HandleError(err)
		case *[]swagger.DescribeResultItem:
			modelResult := resp.(*[]swagger.DescribeResultItem)
			if len(*modelResult) != 1 {
				// this is likely to be unrecoverable
				err := fmt.Errorf("expected only one response from ksql but got %d, \n %v", len(*modelResult), modelResult)
				utilruntime.HandleError(err)
				return nil
			}
			stmtHash := DefaultHasher((*modelResult)[0].SourceDescription.Statement)

			if stmtHash != commandStatus.StatusSha {
				klog.V(4).Info("query stmt differs from what was last seen")
				// clear the command id so that we force re-creation
				commandStatus.CommandID = ""
			}

		}
	case ksqlparser.StmtTypeInsert:
		if err := c.processInsert(ksql, hash, commandStatus); err != nil {
			return err
		}
	default:
		// TODO error unsupported stmt type
		return fmt.Errorf("unsupported stmt type %s", stmt.GetActionType())
	}

	return nil
}

func (c *Controller) processCreateOrReplaceStmt(ksql string, queryHash string, commandStatus *ksqloperatorv1alpha1.CommandStatus) error {
	result, err := c.ksqlClient.CreateDropTerminate(context.Background(), ksql)
	if err != nil {
		// this could be a transient issue so queue for retry
		return err
	}
	switch result.(type) {
	case *swagger.ModelError:
		// this is likely to be unrecoverable
		commandStatus.Status = ksqloperatorv1alpha1.StatusError
		modelErr := result.(*swagger.ModelError)
		err := fmt.Errorf("error response from ksql: (%f0) %s\n%s",
			modelErr.ErrorCode, modelErr.Message, strings.Join(modelErr.StackTrace, "\n"))
		return err
	case *[]swagger.CreateDropTerminateResponseItem:
		modelResult := result.(*[]swagger.CreateDropTerminateResponseItem)
		if len(*modelResult) != 1 {
			// this is likely to be unrecoverable
			err := fmt.Errorf("expected only one response from ksql but got %d, \n %v", len(*modelResult), modelResult)
			return err
		}

		// record the outcome
		response := (*modelResult)[0]
		commandStatus.Status, err = ksqloperatorv1alpha1.ParseCommandStatus(response.CommandStatus.Status)
		if err != nil {
			return err
		}
		commandStatus.CommandID = response.CommandId
		commandStatus.QueryID = response.CommandStatus.QueryId
		commandStatus.StatusSha = DefaultHasher(response.StatementText)
		commandStatus.QuerySha = queryHash

		return nil
	default:
		return fmt.Errorf("unexpected result type %t", result)
	}
}

func (c *Controller) ExecuteInsert(ksql, queryHash string, commandStatus *ksqloperatorv1alpha1.CommandStatus) error {
	// execute this
	result, err := c.ksqlClient.CreateDropTerminate(context.Background(), ksql)
	if err != nil {
		// this could be a transient issue so queue for retry
		return err
	}
	switch result.(type) {
	case *swagger.ModelError:
		// this is likely to be unrecoverable
		commandStatus.Status = ksqloperatorv1alpha1.StatusError
		modelErr := result.(*swagger.ModelError)
		err := fmt.Errorf("error response from ksql: (%f0) %s\n%s",
			modelErr.ErrorCode, modelErr.Message, strings.Join(modelErr.StackTrace, "\n"))
		return err
	case *[]swagger.CreateDropTerminateResponseItem:
		modelResult := result.(*[]swagger.CreateDropTerminateResponseItem)
		if len(*modelResult) != 1 {
			// this is likely to be unrecoverable
			err := fmt.Errorf("expected only one response from ksql but got %d, \n %v", len(*modelResult), modelResult)
			return err
		}

		// record the outcome
		response := (*modelResult)[0]
		commandStatus.Status, err = ksqloperatorv1alpha1.ParseCommandStatus(response.CommandStatus.Status)
		if err != nil {
			return err
		}
		commandStatus.CommandID = response.CommandId
		commandStatus.QueryID = response.CommandStatus.QueryId
		commandStatus.StatusSha = DefaultHasher(response.StatementText)
		commandStatus.QuerySha = queryHash

		return nil
	default:
		utilruntime.HandleError(fmt.Errorf("unexpected result type %t", result))
		return nil
	}
}

func (c *Controller) WaitForSuccess(commandID string) error {
	i := 0
	for ; i < 5; i++ {
		klog.V(5).Infof("query status for %s", commandID)
		resp, err := c.ksqlClient.Status(context.Background(), commandID)
		if err != nil {
			return fmt.Errorf("error getting status for commandID %s: %v", commandID, err)
		}
		switch resp.(type) {
		case *swagger.ModelError:
			modelErr := resp.(*swagger.ModelError)
			if modelErr.ErrorCode == 404 {
				return fmt.Errorf("command %s was not found", commandID)
			}
		case *swagger.StatusResponse:
			response := resp.(*swagger.StatusResponse)

			stat, err := ksqloperatorv1alpha1.ParseCommandStatus(response.Status)
			if err != nil {
				return err
			}
			switch stat {
			case ksqloperatorv1alpha1.StatusQueued:
				continue
			case ksqloperatorv1alpha1.StatusParsing:
				continue
			case ksqloperatorv1alpha1.StatusExecuting:
				continue
			case ksqloperatorv1alpha1.StatusTerminated:
				return fmt.Errorf("command was terminated")
			case ksqloperatorv1alpha1.StatusSuccess:
				return nil
			case ksqloperatorv1alpha1.StatusError:
				return fmt.Errorf("command was errored with message: %s", response.Message)
			}
			time.Sleep(time.Duration(i) * time.Second)
		}
	}
	return fmt.Errorf("status was not resolved in %d retries", i)
}

func (c *Controller) processInsert(ksql string, queryHash string, commandStatus *ksqloperatorv1alpha1.CommandStatus) error {
	klog.V(5).Info("processing insert stmt")
	if commandStatus.QueryID == "" {
		if err := c.ExecuteInsert(ksql, queryHash, commandStatus); err != nil {
			return err
		}
	}

	klog.V(5).Info("explaining insert stmt")
	// lets explain to make sure the state aligns
	result, err := c.ksqlClient.Explain(context.Background(), commandStatus.QueryID)
	if err != nil {
		// likely transient error
		return err
	}

	switch result.(type) {
	case *swagger.ModelError:
		modelErr := result.(*swagger.ModelError)
		if modelErr.ErrorCode == 40001 {
			queryId := commandStatus.QueryID
			commandStatus.QueryID = ""
			return fmt.Errorf("query %s was not found clearing queryID", queryId)
		}
		err := fmt.Errorf("error response from ksql: (%0f) %s\n%s",
			modelErr.ErrorCode, modelErr.Message, strings.Join(modelErr.StackTrace, "\n"))
		utilruntime.HandleError(err)
		return nil
	case *[]swagger.ExplainResultItem:
		result := result.(*[]swagger.ExplainResultItem)
		if len(*result) > 1 {
			// this is likely to be unrecoverable
			utilruntime.HandleError(err)
			return nil
		}
		newStatusSha := DefaultHasher((*result)[0].QueryDescription.StatementText)
		commandStatus.Status, err = ksqloperatorv1alpha1.ParseCommandStatus((*result)[0].QueryDescription.State)
		if err != nil {
			utilruntime.HandleError(err)
			return nil
		}
		// determine if we should terminate the query
		terminate := false
		if commandStatus.StatusSha != newStatusSha {
			// the command has differed from what was issued
			terminate = true
			klog.V(4).Info("query stmt differs from what was last seen")
		}
		if commandStatus.QuerySha != queryHash {
			// the command has changed since we last processed it
			terminate = true
			klog.V(4).Info("query has been modified since last issue")
		}

		if terminate {
			klog.V(5).Infof("terminating %s", commandStatus.QueryID)
			err = c.Terminate(commandStatus.QueryID)
			if err != nil {
				if err != ErrNotFound {
					return err
				}
				// empty the queryID if the query wasn't found
				commandStatus.QueryID = ""
			}
			// issue create
			klog.V(5).Info("issuing insert")
			if err := c.ExecuteInsert(ksql, queryHash, commandStatus); err != nil {
				return err
			}
		}
	}
	return nil
}

const (
	ErrNotFound = Error("not found")
)

func (c *Controller) Terminate(queryID string) error {
	result, err := c.ksqlClient.CreateDropTerminate(context.Background(), fmt.Sprintf("TERMINATE %s;", queryID))
	if err != nil {
		// this could be a transient issue
		return err
	}
	switch result.(type) {
	case *swagger.ModelError:
		//
		modelResult := result.(*swagger.ModelError)
		err := fmt.Errorf("error response from ksql: (%0f) %s\n%s",
			modelResult.ErrorCode, modelResult.Message, strings.Join(modelResult.StackTrace, "\n"))
		if modelResult.ErrorCode == 4001 {
			return ErrNotFound
		}
		return fmt.Errorf("error terminating %s: %v", queryID, err)
	case *[]swagger.CreateDropTerminateResponseItem:
		modelResult := result.(*[]swagger.CreateDropTerminateResponseItem)
		if len(*modelResult) != 1 {
			// this is likely to be unrecoverable
			err := fmt.Errorf("expected only one response from ksql but got %d, \n %v", len(*modelResult), modelResult)
			return err
		}

		if err := c.WaitForSuccess((*modelResult)[0].CommandId); err != nil {
			// an error occurred waiting for the command to be terminated
			// do nothing to the state and expect to hit the 4001 error when we describe it next
			return err
		}
	}
	return nil
}

func (c *Controller) DropTableStreamChain(t string, n string) error {
	resp, err := c.ksqlClient.Describe(context.Background(), n)
	if err != nil {
		return fmt.Errorf("error dropping %s/%s: %v", t, n, err)
	}
	switch (resp).(type) {
	case *swagger.ModelError:
		modelErr := resp.(*swagger.ModelError)
		if modelErr.ErrorCode == ksqlclient.ErrCodeNotFound {
			return ErrNotFound
		}
		err := fmt.Errorf("error response from ksql: (%f0) %s\n%s",
			modelErr.ErrorCode, modelErr.Message, strings.Join(modelErr.StackTrace, "\n"))
		return err
	case *[]swagger.DescribeResultItem:
		modelResult := resp.(*[]swagger.DescribeResultItem)
		if len(*modelResult) != 1 {
			// this is likely to be unrecoverable
			err := fmt.Errorf("expected only one response from ksql but got %d, \n %v", len(*modelResult), modelResult)
			return err
		}

		// queries that write into this table/stream
		for _, q := range (*modelResult)[0].SourceDescription.WriteQueries {
			err := c.Terminate(q.Id)
			if err != nil && err != ErrNotFound {
				return err
			}
		}
		// queries that read from this stream
		for _, q := range (*modelResult)[0].SourceDescription.ReadQueries {
			err := c.Terminate(q.Id)
			if err != nil && err != ErrNotFound {
				return err
			}
		}
	}

	result, err := c.ksqlClient.CreateDropTerminate(context.Background(), fmt.Sprintf("DROP %s %s;", t, n))
	if err != nil {
		return fmt.Errorf("error dropping %s/%s: %v", t, n, err)
	}
	switch (result).(type) {
	case *swagger.ModelError:
		modelErr := result.(*swagger.ModelError)
		err := fmt.Errorf("error response from ksql: (%f0) %s\n%s",
			modelErr.ErrorCode, modelErr.Message, strings.Join(modelErr.StackTrace, "\n"))
		return err
	case *[]swagger.CreateDropTerminateResponseItem:
		modelResult := result.(*[]swagger.CreateDropTerminateResponseItem)
		if len(*modelResult) != 1 {
			// this is likely to be unrecoverable
			err := fmt.Errorf("expected only one response from ksql but got %d, \n %v", len(*modelResult), modelResult)
			return err
		}
		stat, err := ksqloperatorv1alpha1.ParseCommandStatus((*modelResult)[0].CommandStatus.Status)
		if err != nil {
			return err
		}
		switch stat {
		case ksqloperatorv1alpha1.StatusQueued:
			fallthrough
		case ksqloperatorv1alpha1.StatusParsing:
			fallthrough
		case ksqloperatorv1alpha1.StatusExecuting:
			err := c.WaitForSuccess((*modelResult)[0].CommandId)
			if err != nil {
				return err
			}
		case ksqloperatorv1alpha1.StatusTerminated:
			return fmt.Errorf("command was terminated")
		case ksqloperatorv1alpha1.StatusSuccess:
			// noop
		case ksqloperatorv1alpha1.StatusError:
			return fmt.Errorf("command '%s' errored", (*modelResult)[0].CommandId)
		}
	}
	return nil
}
