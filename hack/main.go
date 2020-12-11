// This file is also used by the dockerfile to pre-warm the .cache to speed up builds
package main

import (
	_ "flag"
	"fmt"
	_ "time"

	_ "k8s.io/client-go/informers"
	_ "k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/tools/clientcmd"
	_ "k8s.io/klog/v2"
)

func main() {
	fmt.Println("Hello world!")
}
