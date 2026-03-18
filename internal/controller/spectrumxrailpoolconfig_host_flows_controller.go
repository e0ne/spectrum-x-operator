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
	"github.com/Mellanox/spectrum-x-operator/pkg/exec"
	sriovhosttypes "github.com/k8snetworkplumbingwg/sriov-network-operator/pkg/host/types"
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
	flows    FlowsAPI
	exec     exec.API
	bridge   sriovhosttypes.BridgeInterface
	nodeName string
}

func NewSpectrumXRailPoolConfigHostFlowsReconciler(
	client client.Client,
	flows FlowsAPI,
	execAPI exec.API,
	bridge sriovhosttypes.BridgeInterface,
	nodeName string,
) *SpectrumXRailPoolConfigHostFlowsReconciler {
	return &SpectrumXRailPoolConfigHostFlowsReconciler{
		Client:   client,
		flows:    flows,
		exec:     execAPI,
		bridge:   bridge,
		nodeName: nodeName,
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
	result, err := r.doReconcile(ctx, rpc)
	if apierrors.IsConflict(err) {
		return ctrl.Result{Requeue: true}, nil
	}
	return result, err
}

func (r *SpectrumXRailPoolConfigHostFlowsReconciler) doReconcile(ctx context.Context, rpc *v1alpha1.SpectrumXRailPoolConfig) (ctrl.Result, error) {
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
		policy = r.generateSRIOVNetworkNodePolicy(spec, &rt, false, namespace)
	} else {
		// hw multiplane
		policy = r.generateSRIOVNetworkNodePolicy(spec, &rt, true, namespace)
		if err := r.configureXPlane(ctx, spec, &rt, namespace); err != nil {
			return fmt.Errorf("failed to configure xplane for rail topology %s: %w", rt.Name, err)
		}
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

	pfName := policy.Spec.NicSelector.PfNames[0]

	bridgeName, err := r.flows.GetBridgeNameFromPortName(pfName)
	if err != nil {
		return fmt.Errorf("failed to get bridge name for port %s: %v", pfName, err)
	}

	if len(rt.NicSelector.PfNames) == 1 {
		if err = r.flows.AddSoftwareMultiplaneFlows(
			bridgeName,
			hostFlowsCookie,
			pfName,
		); err != nil {
			return fmt.Errorf("failed to add software multiplane flows: %v", err)
		}
	} else {
		pfNames := policy.Spec.NicSelector.PfNames
		if err := r.flows.AddHardwareMultiplaneGroups(bridgeName, pfNames); err != nil {
			return fmt.Errorf("failed to add hardware multiplane groups: %v", err)
		}

		if err = r.flows.AddHardwareMultiplaneFlows(bridgeName, hostFlowsCookie, pfNames); err != nil {
			return fmt.Errorf("failed to add hardware multiplane flows: %v", err)
		}
	}

	return nil
}

func (r *SpectrumXRailPoolConfigHostFlowsReconciler) configureXPlane(ctx context.Context, spec *v1alpha1.SpectrumXRailPoolConfigSpec, rt *v1alpha1.RailTopology, namespace string) error {
	nodeList := &v1.NodeList{}
	if err := r.Client.List(ctx, nodeList, client.MatchingLabels(spec.NodeSelector)); err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	var localNodeState *sriovv1.SriovNetworkNodeState

	for _, node := range nodeList.Items {
		nodeState := &sriovv1.SriovNetworkNodeState{}
		nsn := types.NamespacedName{Name: node.Name, Namespace: namespace}
		if err := r.Client.Get(ctx, nsn, nodeState); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to get SriovNetworkNodeState for node %s: %w", node.Name, err)
		}
		if nodeState.Status.SyncStatus != v1alpha1.SyncStatusSucceeded {
			return nil
		}
		if node.Name == r.nodeName {
			localNodeState = nodeState
		}
	}

	if localNodeState == nil {
		// local node is not part of this pool
		return nil
	}

	return r.createXPlaneBridges(rt, localNodeState)
}

const xplaneBridge = "br-xplane"

func (r *SpectrumXRailPoolConfigHostFlowsReconciler) createXPlaneBridges(rt *v1alpha1.RailTopology, nodeState *sriovv1.SriovNetworkNodeState) error {
	// Build map of PF name -> interface info from node state
	ifaceByName := make(map[string]*sriovv1.InterfaceExt, len(nodeState.Status.Interfaces))
	for i := range nodeState.Status.Interfaces {
		iface := &nodeState.Status.Interfaces[i]
		ifaceByName[iface.Name] = iface
	}

	// Build desired br-railX bridge configs — one bridge per PF with PF as DPDK uplink.
	// Use ConfigureBridges from sriov-network-operator to manage them via OVSDB.
	desiredBridges := make([]sriovv1.OVSConfigExt, 0, len(rt.NicSelector.PfNames))
	for idx, pfName := range rt.NicSelector.PfNames {
		uplink := sriovv1.OVSUplinkConfigExt{
			Name: pfName,
			Interface: sriovv1.OVSInterfaceConfig{
				Type: ovsNetworkInterfaceType,
			},
		}
		if iface, ok := ifaceByName[pfName]; ok {
			uplink.PciAddress = iface.PciAddress
			if rt.MTU > 0 {
				mtu := rt.MTU
				uplink.Interface.MTURequest = &mtu
			}
		}
		desiredBridges = append(desiredBridges, sriovv1.OVSConfigExt{
			Name: fmt.Sprintf("br-rail%d", idx),
			Bridge: sriovv1.OVSBridgeConfig{
				DatapathType: ovsDataPathType,
			},
			Uplinks: []sriovv1.OVSUplinkConfigExt{uplink},
		})
	}

	currentBridges, err := r.bridge.DiscoverBridges()
	if err != nil {
		return fmt.Errorf("failed to discover bridges: %w", err)
	}

	if err := r.bridge.ConfigureBridges(
		sriovv1.Bridges{OVS: desiredBridges},
		currentBridges,
	); err != nil {
		return fmt.Errorf("failed to configure rail bridges: %w", err)
	}

	// Create br-xplane bridge. It connects all br-railX bridges via patch ports and
	// has no single physical uplink, so it cannot be created via ConfigureBridges.
	if _, err := r.exec.Execute(fmt.Sprintf(
		"ovs-vsctl --may-exist add-br %s -- set bridge %s datapath_type=%s",
		xplaneBridge, xplaneBridge, ovsDataPathType,
	)); err != nil {
		return fmt.Errorf("failed to create bridge %s: %w", xplaneBridge, err)
	}

	// Connect br-xplane to each br-railX via patch ports and add VF representors.
	for idx, pfName := range rt.NicSelector.PfNames {
		railBridge := fmt.Sprintf("br-rail%d", idx)
		patchXplanePort := fmt.Sprintf("patch-xplane-to-rail%d", idx)
		patchRailPort := fmt.Sprintf("patch-rail%d-to-xplane", idx)

		if _, err := r.exec.Execute(fmt.Sprintf(
			"ovs-vsctl --may-exist add-port %s %s -- set Interface %s type=patch options:peer=%s",
			xplaneBridge, patchXplanePort, patchXplanePort, patchRailPort,
		)); err != nil {
			return fmt.Errorf("failed to add patch port %s to bridge %s: %w", patchXplanePort, xplaneBridge, err)
		}

		if _, err := r.exec.Execute(fmt.Sprintf(
			"ovs-vsctl --may-exist add-port %s %s -- set Interface %s type=patch options:peer=%s",
			railBridge, patchRailPort, patchRailPort, patchXplanePort,
		)); err != nil {
			return fmt.Errorf("failed to add patch port %s to bridge %s: %w", patchRailPort, railBridge, err)
		}

		if iface, ok := ifaceByName[pfName]; ok {
			for _, vf := range iface.VFs {
				if vf.RepresentorName == "" {
					continue
				}
				if _, err := r.exec.Execute(fmt.Sprintf(
					"ovs-vsctl --may-exist add-port %s %s",
					railBridge, vf.RepresentorName,
				)); err != nil {
					return fmt.Errorf("failed to add representor %s to bridge %s: %w", vf.RepresentorName, railBridge, err)
				}
			}
		}
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

	newStatus := v1alpha1.SyncStatusSucceeded
	for _, node := range nodeList.Items {
		nodeState := &sriovv1.SriovNetworkNodeState{}
		nsn := types.NamespacedName{Name: node.Name, Namespace: rpc.Namespace}
		if err := r.Client.Get(ctx, nsn, nodeState); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("failed to get SriovNetworkNodeState for node %s: %w", node.Name, err)
		}
		switch nodeState.Status.SyncStatus {
		case v1alpha1.SyncStatusFailed:
			return r.patchSyncStatus(ctx, rpc, v1alpha1.SyncStatusFailed)
		case v1alpha1.SyncStatusInProgress:
			newStatus = v1alpha1.SyncStatusInProgress
		}
	}

	return r.patchSyncStatus(ctx, rpc, newStatus)
}

func (r *SpectrumXRailPoolConfigHostFlowsReconciler) patchSyncStatus(ctx context.Context, rpc *v1alpha1.SpectrumXRailPoolConfig, newStatus string) error {
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
			NodeSelector:             nodeSelector,
			RdmaMode:                 "exclusive",
			OvsHardwareOffloadConfig: sriovv1.OvsHardwareOffloadConfig{
				// TODO: otherConfig option
			},
		},
	}

	return nodePool
}

func (r *SpectrumXRailPoolConfigHostFlowsReconciler) generateSRIOVNetworkNodePolicy(spec *v1alpha1.SpectrumXRailPoolConfigSpec, rt *v1alpha1.RailTopology, hardwarePLB bool, namespace string) *sriovv1.SriovNetworkNodePolicy {
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
	if !hardwarePLB {
		bridge := &sriovv1.Bridge{
			GroupingPolicy: "perPF",
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
