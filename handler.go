package main

/*
import (
	"context"
	"fmt"

	"k8s.io/client-go/tools/record"

	"ksql_operator/ksqlclient"
	"ksql_operator/ksqlclient/swagger"
	samplev1alpha1 "ksql_operator/pkg/apis/ksql_operator/v1alpha1"

	corev1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

type ConnectorHandler struct {
	resource     *samplev1alpha1.Connector
	connection   *samplev1alpha1.Connection
	kSQLClient   ksqlclient.KSQLClient
	recorder     record.EventRecorder
	updateStatus bool
}

func newHandler(managedResource *samplev1alpha1.Connector, connection *samplev1alpha1.Connection, kSQLClient ksqlclient.KSQLClient, recorder record.EventRecorder) *ConnectorHandler {
	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance
	managedResourceCopy := managedResource.DeepCopy()
	return &ConnectorHandler{
		resource:   managedResourceCopy,
		connection: connection,
		kSQLClient: kSQLClient,
		recorder:   recorder,
	}
}

func (h *ConnectorHandler) Handle() error {

	managedResource := h.resource
	// lets validate the resource to ensure its valid
	if err, msg := ValidateManagedResourceSpec(managedResource); err != nil {
		utilruntime.HandleError(fmt.Errorf("%s: %s", err, msg))
		h.updateStatus = true
		h.resource.Status.Applied = samplev1alpha1.ResourceStatusFailed
		// lets make sure we dont requeue the item
		return nil
	}

	// lets see if it exists by describing it managedResource.Spec.Type
	resp, err := h.kSQLClient.Describe(context.Background(), h.connection.Spec.Uri, managedResource.Spec.Identifier)
	if err != nil {
		msg := fmt.Sprintf(MessageExecutingKSQL, managedResource.Identifier, managedResource.Namespace, err)
		h.recorder.Event(managedResource, corev1.EventTypeWarning, ErrExecutingKSQL, msg)
		h.updateStatus = true
		h.resource.Status.Applied = samplev1alpha1.ResourceStatusPending
		return fmt.Errorf(msg)
	}

	ksqlStatement := GenerateCreateStatement(managedResource)
	if badRequest, ok := (resp).(*swagger.ModelError); ok {
		return h.handleManagedResourceNotFound(badRequest, ksqlStatement)
	}

	if desc, ok := (resp).(*swagger.DescribeResult); ok {
		if len(*desc) != 1 {
			// we were only expecting one result
			// TODO error and return
		}
		return h.handleManagedResourceCreated()
	}

	// if we get here there was an expected response type from the swagger client
	msg := fmt.Sprintf(MessageUnexpectedResponseType, resp)
	h.recorder.Event(managedResource, corev1.EventTypeWarning, ErrUnexpectedResponseType, msg)
	h.updateStatus = true
	h.resource.Status.Applied = samplev1alpha1.ResourceStatusFailed
	return nil
}

func (h *ConnectorHandler) handleManagedResourceCreated() error {
	h.recorder.Event(h.resource, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	// Finally, we update the status block of the Connector resource to reflect the
	// current state of the world
	if h.resource.Status.Applied != samplev1alpha1.ResourceStatusApplied {
		h.updateStatus = true
	}
	h.resource.Status.Applied = samplev1alpha1.ResourceStatusApplied

	if h.resource.Status.RunningQueries == nil {
		h.resource.Status.RunningQueries = map[string]samplev1alpha1.RunningQueryStatus{}
	}

	// kick off any/all of the runningQueries
	for k, v := range h.resource.Spec.RunningQueries {
		// check the status
		if stat, ok := h.resource.Status.RunningQueries[k]; ok {
			if stat.QueryID != "" {
				// we've executed this previously
				// TODO describe it to get the "status"
				continue
			}
		}
		// theres no status so we need to create it
		ksqlStatement := GenerateRunningQuery(h.resource, v)
		res, err := h.kSQLClient.CreateDropTerminate(context.Background(), h.connection.Spec.Uri, ksqlStatement)
		if err != nil {
			return h.handleKsqlExecError(err)
		}
		if createDropTerminateResponse, ok := (res).(*swagger.CreateDropTerminateResponse); ok {
			// TODO length check createDropTerminateResponse
			responseItem := (*createDropTerminateResponse)[0]
			h.updateStatus = true
			stat := samplev1alpha1.RunningQueryStatus{
				Status: responseItem.CommandStatus.Status,
			}
			err, done := h.handleRunningQueryCreateResponse(responseItem, &stat, k)
			h.resource.Status.RunningQueries[k] = stat
			if done {
				return err
			}
		}
		if bres, ok := (res).(*swagger.ModelError); ok {
			return h.handleKSQLUnexpectedError(bres)
		}
	}
	return nil
}

func (h *ConnectorHandler) handleRunningQueryCreateResponse(responseItem swagger.CreateDropTerminateResponseItem, stat *samplev1alpha1.RunningQueryStatus, k string) (error, bool) {
	switch responseItem.CommandStatus.Status {
	case ksqlclient.CommandStatusSuccess:
		//stick the id in the status
		//
		id := ExtractQueryID(responseItem.CommandStatus.Message)
		if id == "" {
			msg := fmt.Sprintf(MessageUnableToExtractQueryID, responseItem.CommandStatus.Message)
			h.recorder.Event(h.resource, corev1.EventTypeWarning, ErrUnableToExtractQueryID, msg)
			h.updateStatus = true
			h.resource.Status.Applied = samplev1alpha1.ResourceStatusFailed
			// we want to retry so return nil
			return nil, true
		}
		stat.QueryID = id
		msg := fmt.Sprintf(MessageRunningQuerySynced, k)
		h.recorder.Event(h.resource, corev1.EventTypeNormal, SuccessSynced, msg)
		h.updateStatus = true
		h.resource.Status.Applied = samplev1alpha1.ResourceStatusApplied
		return nil, false
	case ksqlclient.CommandStatusError:
		msg := fmt.Sprintf(MessageExecutingKSQL, responseItem.CommandStatus.Message)
		h.recorder.Event(h.resource, corev1.EventTypeWarning, ErrExecutingKSQL, msg)

		h.updateStatus = true
		h.resource.Status.Applied = samplev1alpha1.ResourceStatusPending
		return fmt.Errorf(msg), true
	case ksqlclient.CommandStatusExecuting:
		fallthrough
	case ksqlclient.CommandStatusQueued:
		fallthrough
	case ksqlclient.CommandStatusTerminated:
		msg := fmt.Sprintf(MessagePending, responseItem.CommandStatus.Status, responseItem.CommandStatus.Message)
		h.recorder.Event(h.resource, corev1.EventTypeNormal, Pending, msg)
		h.updateStatus = true
		h.resource.Status.Applied = samplev1alpha1.ResourceStatusPending
		return fmt.Errorf(msg), true
	default:
		h.recorder.Event(h.resource, corev1.EventTypeWarning, ErrKSQLUnexpected,
			fmt.Sprintf("Unhandled command status type '%s'", responseItem.CommandStatus))

		h.updateStatus = true
		h.resource.Status.Applied = samplev1alpha1.ResourceStatusFailed
		return nil, true
	}
	return nil, false
}


func (h *ConnectorHandler) handleManagedResourceNotFound(badRequest *swagger.ModelError, ksqlStatement string) error {
	if badRequest.ErrorCode == ksqlclient.ErrorCodeNotFound {
		// the table/stream doesn't exist we create it!
		res, err := h.kSQLClient.CreateDropTerminate(context.Background(), h.connection.Spec.Uri, ksqlStatement)
		if err != nil {
			h.updateStatus = true
			h.resource.Status.Applied = samplev1alpha1.ResourceStatusPending
			return h.handleKsqlExecError(err)
		}
		if createDropTerminateResponse, ok := (res).(*swagger.CreateDropTerminateResponse); ok {
			// TODO length check
			createDropTerminateResponseItem := (*createDropTerminateResponse)[0]
			switch createDropTerminateResponseItem.CommandStatus.Status {
			case ksqlclient.CommandStatusSuccess:
				h.recorder.Event(h.resource, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
				return h.handleManagedResourceCreated()

			case ksqlclient.CommandStatusError:
				msg := fmt.Sprintf(MessageExecutingKSQL, createDropTerminateResponseItem.CommandStatus.Message)
				h.recorder.Event(h.resource, corev1.EventTypeWarning, ErrExecutingKSQL, msg)
				h.updateStatus = true
				h.resource.Status.Applied = samplev1alpha1.ResourceStatusPending
				return fmt.Errorf(msg)
			case ksqlclient.CommandStatusExecuting:
				fallthrough
			case ksqlclient.CommandStatusQueued:
				fallthrough
			case ksqlclient.CommandStatusTerminated:
				msg := fmt.Sprintf(MessagePending, createDropTerminateResponseItem.CommandStatus.Status, createDropTerminateResponseItem.CommandStatus.Message)
				h.recorder.Event(h.resource, corev1.EventTypeNormal, Pending, msg)
				h.updateStatus = true
				h.resource.Status.Applied = samplev1alpha1.ResourceStatusPending
				return fmt.Errorf(msg)
			default:
				h.recorder.Event(h.resource, corev1.EventTypeWarning, ErrKSQLUnexpected,
					fmt.Sprintf("Unhandled command status type '%s'", createDropTerminateResponseItem.CommandStatus))
				h.updateStatus = true
				h.resource.Status.Applied = samplev1alpha1.ResourceStatusFailed
				return nil
			}
		}
		if bres, ok := (res).(*swagger.ModelError); ok {
			return h.handleKSQLUnexpectedError(bres)
		}
		// else fallthrough to the // something unexpected!
	}
	return h.handleKSQLUnexpectedError(badRequest)
}

func (h *ConnectorHandler) handleKsqlExecError(err error) error {
	msg := fmt.Sprintf(MessageExecutingKSQL, err)
	h.recorder.Event(h.resource, corev1.EventTypeWarning, ErrExecutingKSQL, msg)
	h.updateStatus = true
	h.resource.Status.Applied = samplev1alpha1.ResourceStatusPending
	return fmt.Errorf(msg)
}

func (h *ConnectorHandler) handleKSQLUnexpectedError(badRequest *swagger.ModelError) error {
	// something unexpected!
	msg := fmt.Sprintf(MessageKSQLUnexpected, badRequest.Message, badRequest.ErrorCode)
	h.recorder.Event(h.resource, corev1.EventTypeWarning, ErrKSQLUnexpected, msg)

	h.updateStatus = true
	h.resource.Status.Applied = samplev1alpha1.ResourceStatusPending
	return fmt.Errorf(msg)
}
*/
