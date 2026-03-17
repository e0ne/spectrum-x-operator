/*
 Copyright 2025, NVIDIA CORPORATION & AFFILIATES

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

package controller

import (
	"context"
	"fmt"
	"github.com/Mellanox/spectrum-x-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sriovv1 "github.com/k8snetworkplumbingwg/sriov-network-operator/api/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

const hostFlowsCookie uint64 = 0x2

const SpectrumXRailPoolConfigControllerName = "SpectrumXRailPoolConfigController"

const (
	sriovNodePolicyType     = "SriovNetworkNodePolicy"
	sriovNetworkPoolConfig  = "SriovNetworkPoolConfig"
	sriovOVSNetworkType     = "OVSNetwork"
	ovsDataPathType         = "netdev"
	ovsNetworkInterfaceType = "dpdk"
)

const (
	finalizerName  = "spectrumx.nvidia.com/spectrumxrailpoolconfig"
	labelOwnerName = "spectrumx.nvidia.com/owner-name"
)

// SpectrumXRailPoolConfigHostFlowsReconciler reconciles a SpectrumXRailPoolConfig object
type SpectrumXRailPoolConfigHostFlowsReconciler struct {
	client.Client
	flows FlowsAPI
}

func NewSpectrumXRailPoolConfigHostFlowsReconciler(
	client client.Client,
	flows FlowsAPI,
) *SpectrumXRailPoolConfigHostFlowsReconciler {
	return &SpectrumXRailPoolConfigHostFlowsReconciler{
		Client: client,
		flows:  flows,
	}
}

// +kubebuilder:rbac:groups=spectrumx.nvidia.com,resources=spectrumxrailpoolconfigs,verbs=get;list;watch
// +kubebuilder:rbac:groups=spectrumx.nvidia.com,resources=spectrumxrailpoolconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sriovnetwork.openshift.io,resources=sriovnetworknodepolicies,verbs=create;patch;get;list;watch;update;delete
// +kubebuilder:rbac:groups=sriovnetwork.openshift.io,resources=sriovnetworkpoolconfigs,verbs=create;patch;get;list;watch;update;delete
// +kubebuilder:rbac:groups=sriovnetwork.openshift.io,resources=ovsnetworks,verbs=create;patch;get;list;watch;update;delete
// +kubebuilder:rbac:groups=sriovnetwork.openshift.io,resources=sriovnetworknodestates,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the SpectrumXRailPoolConfig object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *SpectrumXRailPoolConfigHostFlowsReconciler) Reconcile(ctx context.Context, rpc *v1alpha1.SpectrumXRailPoolConfig) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(rpc, finalizerName) {
		patch := client.MergeFrom(rpc.DeepCopy())
		controllerutil.AddFinalizer(rpc, finalizerName)
		return ctrl.Result{}, r.Client.Patch(ctx, rpc, patch)
	}

	if !rpc.DeletionTimestamp.IsZero() {
		for _, rt := range rpc.Spec.RailTopology {
			if err := r.deleteRailTopologyResources(ctx, rpc.Namespace, rt.Name); err != nil {
				log.Error(err, "failed to delete rail topology resources", "rail topology", rt)
				return ctrl.Result{}, err
			}
		}
		patch := client.MergeFrom(rpc.DeepCopy())
		controllerutil.RemoveFinalizer(rpc, finalizerName)
		return ctrl.Result{}, r.Client.Patch(ctx, rpc, patch)
	}

	if rpc.Status.SyncStatus != v1alpha1.SyncStatusInProgress {
		patch := client.MergeFrom(rpc.DeepCopy())
		rpc.Status.SyncStatus = v1alpha1.SyncStatusInProgress
		if err := r.Client.Status().Patch(ctx, rpc, patch); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to set SyncStatus to InProgress: %w", err)
		}
		return ctrl.Result{}, nil
	}

	if len(rpc.Spec.RailTopology) < 1 {
		return ctrl.Result{}, fmt.Errorf("expected one or more rail topologies to be specified")
	}

	for _, rt := range rpc.Spec.RailTopology {
		err := r.reconcileRailTopology(&rpc.Spec, rt, rpc.Namespace, rpc.Name)
		if err != nil {
			log.Error(err, "failed to reconcile rail topology", "rail topology", rt)
			return ctrl.Result{}, err
		}
	}

	if err := r.deleteRemovedRailTopologies(ctx, rpc); err != nil {
		log.Error(err, "failed to delete removed rail topologies")
		return ctrl.Result{}, err
	}

	if err := r.updateSyncStatus(ctx, rpc); err != nil {
		log.Error(err, "failed to update sync status")
		return ctrl.Result{}, err
	}

	//// Get the SriovNetworkNodePolicy
	//nsn := types.NamespacedName{Namespace: rpc.Namespace, Name: rpc.Spec.SriovNetworkNodePolicyRef}
	//sriovNetworkNodePolicy := &sriovv1.SriovNetworkNodePolicy{}

	//if err := r.Client.Get(ctx, nsn, sriovNetworkNodePolicy); err != nil {
	//	return ctrl.Result{}, fmt.Errorf("failed to get SriovNetworkNodePolicy %s: %v", nsn, err)
	//}

	//if len(sriovNetworkNodePolicy.Spec.NicSelector.PfNames) < 1 {
	//	return ctrl.Result{}, fmt.Errorf("expected 1 PF name in SriovNetworkNodePolicy, got %d", len(sriovNetworkNodePolicy.Spec.NicSelector.PfNames))
	//}

	//var (
	//	bridgeName string
	//	err        error
	//)

	//pfName := sriovNetworkNodePolicy.Spec.NicSelector.PfNames[0]
	//
	//bridgeName, err = r.flows.GetBridgeNameFromPortName(pfName)
	//if err != nil {
	//	return ctrl.Result{}, fmt.Errorf("failed to get bridge name for port %s: %v", pfName, err)
	//}
	//
	//// TODO: Add a cleanup mechanism.
	//// Because we have no finalizer for the SpectrumXRailPoolConfig, we might miss the deletion event
	//// and not cleanup the flows.
	//if rpc.DeletionTimestamp != nil {
	//	// Delete the flows
	//	return ctrl.Result{}, r.flows.DeleteFlowsByCookie(bridgeName, hostFlowsCookie)
	//}
	//
	//switch rpc.Spec.MultiplaneMode {
	//case "none", "swplb":
	//	if err = r.flows.AddSoftwareMultiplaneFlows(
	//		bridgeName,
	//		hostFlowsCookie,
	//		sriovNetworkNodePolicy.Spec.NicSelector.PfNames[0],
	//	); err != nil {
	//		return ctrl.Result{}, fmt.Errorf("failed to add software multiplane flows: %v", err)
	//	}
	//case "hwplb":
	//	pfNames := sriovNetworkNodePolicy.Spec.NicSelector.PfNames
	//
	//	if err := r.flows.AddHardwareMultiplaneGroups(bridgeName, pfNames); err != nil {
	//		return ctrl.Result{}, fmt.Errorf("failed to add hardware multiplane groups: %v", err)
	//	}
	//
	//	if err = r.flows.AddHardwareMultiplaneFlows(bridgeName, hostFlowsCookie, pfNames); err != nil {
	//		return ctrl.Result{}, fmt.Errorf("failed to add hardware multiplane flows: %v", err)
	//	}
	//default:
	//	log.Info("Unhandled multiplane mode", "mode", rpc.Spec.MultiplaneMode)
	//	return ctrl.Result{}, nil
	//}

	return ctrl.Result{}, nil
}

func (r *SpectrumXRailPoolConfigHostFlowsReconciler) reconcileRailTopology(spec *v1alpha1.SpectrumXRailPoolConfigSpec, rt v1alpha1.RailTopology, namespace, rpcName string) error {
	ctx := context.TODO()
	if len(rt.NicSelector.PfNames) == 0 {
		return fmt.Errorf("no PF names are cpecified in rail topology")
	}

	ownerLabels := map[string]string{labelOwnerName: rpcName}

	poolConfig := r.generateSRIOVNetworkPoolConfig(spec, &rt, namespace)
	poolConfig.SetGroupVersionKind(sriovv1.GroupVersion.WithKind(sriovNetworkPoolConfig))
	poolConfig.Labels = ownerLabels
	if err := r.Client.Patch(ctx, poolConfig, client.Apply, client.ForceOwnership, client.FieldOwner(SpectrumXRailPoolConfigControllerName)); err != nil {
		return fmt.Errorf("error while patching %s %s: %w", poolConfig.GetObjectKind().GroupVersionKind().String(), client.ObjectKeyFromObject(poolConfig), err)
	}

	var policy *sriovv1.SriovNetworkNodePolicy
	if len(rt.NicSelector.PfNames) == 1 {
		// sw plb or no multiplane
		policy = r.generateSRIOVNetworkNodePolicy(spec, &rt, true, namespace)
	} else {
		// hw multiplane
		policy = r.generateSRIOVNetworkNodePolicy(spec, &rt, false, namespace)
	}
	policy.Labels = ownerLabels

	if err := r.Client.Patch(ctx, policy, client.Apply, client.ForceOwnership, client.FieldOwner(SpectrumXRailPoolConfigControllerName)); err != nil {
		return fmt.Errorf("error while patching %s %s: %w", policy.GetObjectKind().GroupVersionKind().String(), client.ObjectKeyFromObject(policy), err)
	}

	addBridge := len(rt.NicSelector.PfNames) == 1
	ovsNetwork := r.generateOVSNetwork(spec, &rt, addBridge, namespace)
	ovsNetwork.SetGroupVersionKind(sriovv1.GroupVersion.WithKind(sriovOVSNetworkType))
	ovsNetwork.Labels = ownerLabels
	if err := r.Client.Patch(ctx, ovsNetwork, client.Apply, client.ForceOwnership, client.FieldOwner(SpectrumXRailPoolConfigControllerName)); err != nil {
		return fmt.Errorf("error while patching %s %s: %w", ovsNetwork.GetObjectKind().GroupVersionKind().String(), client.ObjectKeyFromObject(ovsNetwork), err)
	}

	return nil
}

func (r *SpectrumXRailPoolConfigHostFlowsReconciler) deleteRailTopologyResources(ctx context.Context, namespace, rtName string) error {
	policy := &sriovv1.SriovNetworkNodePolicy{ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: namespace}}
	if err := r.Client.Delete(ctx, policy); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to delete SriovNetworkNodePolicy %s/%s: %w", namespace, rtName, err)
	}

	poolConfig := &sriovv1.SriovNetworkPoolConfig{ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: namespace}}
	if err := r.Client.Delete(ctx, poolConfig); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to delete SriovNetworkPoolConfig %s/%s: %w", namespace, rtName, err)
	}

	ovsNetwork := &sriovv1.OVSNetwork{ObjectMeta: metav1.ObjectMeta{Name: rtName, Namespace: namespace}}
	if err := r.Client.Delete(ctx, ovsNetwork); client.IgnoreNotFound(err) != nil {
		return fmt.Errorf("failed to delete OVSNetwork %s/%s: %w", namespace, rtName, err)
	}

	return nil
}

func (r *SpectrumXRailPoolConfigHostFlowsReconciler) deleteRemovedRailTopologies(ctx context.Context, rpc *v1alpha1.SpectrumXRailPoolConfig) error {
	currentTopologies := make(map[string]struct{}, len(rpc.Spec.RailTopology))
	for _, rt := range rpc.Spec.RailTopology {
		currentTopologies[rt.Name] = struct{}{}
	}

	policyList := &sriovv1.SriovNetworkNodePolicyList{}
	if err := r.Client.List(ctx, policyList,
		client.InNamespace(rpc.Namespace),
		client.MatchingLabels{labelOwnerName: rpc.Name},
	); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to list SriovNetworkNodePolicies: %w", err)
	}

	for _, policy := range policyList.Items {
		if _, exists := currentTopologies[policy.Name]; !exists {
			if err := r.deleteRailTopologyResources(ctx, rpc.Namespace, policy.Name); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *SpectrumXRailPoolConfigHostFlowsReconciler) updateSyncStatus(ctx context.Context, rpc *v1alpha1.SpectrumXRailPoolConfig) error {
	nodeList := &v1.NodeList{}
	if err := r.Client.List(ctx, nodeList, client.MatchingLabels(rpc.Spec.NodeSelector)); err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	newStatus := v1alpha1.SyncStatusInProgress
	for _, node := range nodeList.Items {
		nodeState := &sriovv1.SriovNetworkNodeState{}
		nsn := types.NamespacedName{Name: node.Name, Namespace: rpc.Namespace}
		if err := r.Client.Get(ctx, nsn, nodeState); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("failed to get SriovNetworkNodeState for node %s: %w", node.Name, err)
		}
		if nodeState.Status.SyncStatus == v1alpha1.SyncStatusFailed {
			newStatus = v1alpha1.SyncStatusFailed
			break
		}
	}

	if rpc.Status.SyncStatus == newStatus {
		return nil
	}
	patch := client.MergeFrom(rpc.DeepCopy())
	rpc.Status.SyncStatus = newStatus
	return r.Client.Status().Patch(ctx, rpc, patch)
}

func (r *SpectrumXRailPoolConfigHostFlowsReconciler) generateSRIOVNetworkPoolConfig(spec *v1alpha1.SpectrumXRailPoolConfigSpec, rt *v1alpha1.RailTopology, namespace string) *sriovv1.SriovNetworkPoolConfig {
	nodeSelector := &metav1.LabelSelector{
		MatchLabels: spec.NodeSelector,
	}

	nodePool := &sriovv1.SriovNetworkPoolConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rt.Name,
			Namespace: namespace,
		},
		Spec: sriovv1.SriovNetworkPoolConfigSpec{
			NodeSelector: nodeSelector,
			RdmaMode:     "exclusive",
			OvsHardwareOffloadConfig: sriovv1.OvsHardwareOffloadConfig{
				// TODO: otherConfig option
			},
		},
	}

	return nodePool
}

func (r *SpectrumXRailPoolConfigHostFlowsReconciler) generateSRIOVNetworkNodePolicy(spec *v1alpha1.SpectrumXRailPoolConfigSpec, rt *v1alpha1.RailTopology, generateBridge bool, namespace string) *sriovv1.SriovNetworkNodePolicy {
	nicSelector := &sriovv1.SriovNetworkNicSelector{
		PfNames: rt.NicSelector.PfNames,
	}
	nodeSelector := spec.NodeSelector

	nodePolicy := &sriovv1.SriovNetworkNodePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rt.Name,
			Namespace: namespace,
		},
		// According to NVIDIA Spectrum-X architecture we need only VF per PF to be created
		// which would be used for GPU to GPU traffic so IsRDMA flag is required
		// NOTE: Temporary ignore MTU type conversion until we address it in SR-IOV Network Operator
		//nolint:gosec
		Spec: sriovv1.SriovNetworkNodePolicySpec{
			ResourceName: rt.Name,
			Mtu:          rt.MTU,
			NumVfs:       spec.NumVfs,
			NicSelector:  *nicSelector,
			NodeSelector: nodeSelector,
			IsRdma:       true,
			EswitchMode:  "switchdev",
			////Bridge:       *bridge,
			//Bridge: nil,
		},
	}
	if generateBridge {
		bridge := &sriovv1.Bridge{
			OVS: &sriovv1.OVSConfig{

				Bridge: sriovv1.OVSBridgeConfig{
					DatapathType: ovsDataPathType,
					// TODO: groupingPolicy=perPF option
				},
				Uplink: sriovv1.OVSUplinkConfig{
					Interface: sriovv1.OVSInterfaceConfig{
						Type:       ovsNetworkInterfaceType,
						MTURequest: &rt.MTU,
					},
				},
			},
		}

		nodePolicy.Spec.Bridge = *bridge
	}

	nodePolicy.ObjectMeta.ManagedFields = nil
	nodePolicy.SetGroupVersionKind(sriovv1.GroupVersion.WithKind(sriovNodePolicyType))
	return nodePolicy
}

func (r *SpectrumXRailPoolConfigHostFlowsReconciler) generateOVSNetwork(spec *v1alpha1.SpectrumXRailPoolConfigSpec, rt *v1alpha1.RailTopology, addBridge bool, namespace string) *sriovv1.OVSNetwork {
	ovsNetwork := &sriovv1.OVSNetwork{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rt.Name,
			Namespace: namespace,
		},
		Spec: sriovv1.OVSNetworkSpec{
			ResourceName:     rt.Name,
			InterfaceType:    ovsNetworkInterfaceType,
			NetworkNamespace: spec.NetworkNamespace,
			MTU:              uint(rt.MTU),
			IPAM: `
				"type": "nv-ipam",
				"poolName": "rail-1",
				"poolType": "cidrpool"
				}
					metaPlugins: |
				{
					"type": "rdma"
				},
				{
					"type": "rail"
				}`,
		},
	}
	if addBridge {
		ovsNetwork.Spec.Bridge = fmt.Sprintf("br-%s", rt.Name)
	}
	return ovsNetwork
}

// SetupWithManager sets up the controller with the Manager.
func (r *SpectrumXRailPoolConfigHostFlowsReconciler) SetupWithManager(
	mgr ctrl.Manager,
	nodeName string,
	ovsWatcher <-chan event.GenericEvent,
) error {
	railListerHandler := handler.EnqueueRequestsFromMapFunc(NewNodeRailLister(r.Client, nodeName).ListRailPoolConfigsForNode)

	nodeNameFilter := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return nodeName == obj.GetName()
	})

	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.SpectrumXRailPoolConfig{}). // TODO: only reconcile objects that are related to this node
		Watches(
			&v1.Node{},
			railListerHandler,
			builder.WithPredicates(
				predicate.And(predicate.LabelChangedPredicate{}, nodeNameFilter),
			),
		).
		WatchesRawSource(source.Channel(ovsWatcher, railListerHandler)).
		Named("spectrumxrailpoolconfig-host-flows").Complete(reconcile.AsReconciler[*v1alpha1.SpectrumXRailPoolConfig](r.Client, r))
}

type nodeRailLister struct {
	client   client.Client
	nodeName string
}

func NewNodeRailLister(client client.Client, nodeName string) *nodeRailLister {
	return &nodeRailLister{client: client, nodeName: nodeName}
}

func (r *nodeRailLister) ListRailPoolConfigsForNode(ctx context.Context, _ client.Object) []reconcile.Request {
	logger := log.FromContext(ctx)

	node := &v1.Node{}
	if err := r.client.Get(ctx, types.NamespacedName{Name: r.nodeName}, node); err != nil {
		return nil
	}

	list := &v1alpha1.SpectrumXRailPoolConfigList{}
	if err := r.client.List(ctx, list); err != nil {
		logger.Error(err, "failed to list SpectrumXRailPoolConfigs")
		return nil
	}

	requests := make([]reconcile.Request, 0)

	for _, rpc := range list.Items {
		for _, rt := range rpc.Spec.RailTopology {
			// Get the SriovNetworkNodePolicy
			nsn := types.NamespacedName{Namespace: rpc.Namespace, Name: rt.Name}
			snnp := sriovv1.SriovNetworkNodePolicy{}

			if err := r.client.Get(ctx, nsn, &snnp); err != nil {
				logger.Error(err, "failed to get SriovNetworkNodePolicy", "nsn", nsn)
				continue
			}

			// If the SriovNetworkNodePolicy selects this node, add the SpectrumXRailPoolConfig to the requests
			if labels.Set(snnp.Spec.NodeSelector).AsSelector().Matches(labels.Set(node.Labels)) {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{Namespace: rpc.Namespace, Name: rpc.Name},
				})
			}
		}
	}

	return requests
}
