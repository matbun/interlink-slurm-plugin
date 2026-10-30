---
sidebar_position: 4
---

# Multi-pod jobs and site launchers

With `GangSchedulingEnabled: true`, the pods of one distributed job (a Kubeflow Trainer
TrainJob, a KubeRay RayCluster, a JobSet) start as **one** Slurm job: all of them
together on N nodes, or none.

## Which pods form a gang

The plugin reads Kueue's pod-group keys, whoever writes them (Kueue, a policy engine,
the user):

| Key | Kind | Meaning |
|---|---|---|
| `kueue.x-k8s.io/pod-group-name` | label | name of the group, unique per job |
| `kueue.x-k8s.io/pod-group-total-count` | annotation | number of pods in the group |

The rank comes from `batch.kubernetes.io/job-completion-index`; the head is the pod with
`ray.io/node-type: head` or the JobSet replicated job `launcher`. The plugin buffers the
members until the group is complete (`GangTimeout`, default 15m), then submits one
`sbatch --nodes=<count>`.

A caller that holds the whole group can skip the buffer: `POST /createPodSets` takes
`{"groupUID": ..., "podSets": [{"name": ..., "pods": [...]}]}`, each pod as `/create`
receives it, and submits the same job at once. Ranks follow the order of the pods, PodSet
after PodSet, and rank 0 is the head. The answer lists one `PodUID`/`PodJID` per pod;
status, logs and delete stay per pod. The ping answer lists the capability
`podset-create` when `GangSchedulingEnabled` is set.

## How the job starts: the site file

The same distributed job starts differently at every site: modules to load, `srun` or
`mpirun`, PMIx flavour, fabric libraries, NCCL and UCX settings. The site administrator
keeps that knowledge in ONE bash file, `SiteScriptPath` (default
`/etc/interlink/site/site.sh`, typically a mounted ConfigMap):

- its top level runs in the batch shell at the start of every gang job;
- each `<name>-launcher` function starts a gang whose pods carry
  `interlink.eu/launcher: <name>`.

The function is called once, from the batch script on the head's node, with the head
pod's command as arguments. It starts every rank itself (`srun`, `mpirun`) and its return
code ends every pod of the gang. Without `interlink.eu/launcher`, each pod runs its own
command on its own node.

The plugin exports, for the function:

| Variable | Value |
|---|---|
| `GANG_NODES` | allocated hosts, in job order (space separated) |
| `GANG_NNODES` | number of nodes |
| `GANG_HEAD_NODE` | the head's host (the batch node) |
| `GANG_GPUS_PER_NODE` | GPUs per node, from `--gres=gpu:N` or `--gpus-per-node` |
| `MASTER_ADDR`, `MASTER_PORT` | the head's address and a coordination port |
| `GANG_CONTAINER` | the head pod's container exec prefix (runtime options and image), to use unquoted |
| `GANG_LOG_DIR` | rank i's log, shown as pod i's log, is `$GANG_LOG_DIR/rank-<i>.out`; whatever the function prints itself goes to rank 0 |
| `INTERLINK_SOCKET_DIR` | the head pod's socket directory, when the site has one |

With `srun`, `--output="$GANG_LOG_DIR/rank-%n.out" --open-mode=append` gives every pod its
own log. A comment line `# interlink-tasks-per-gpu: <name> ...` makes those launchers get
one Slurm task per GPU (for `mpirun`, which takes its slots from the allocation); the
others get one task per node.

```bash
# interlink-tasks-per-gpu: mpi

module load singularity
export SINGULARITYENV_NCCL_SOCKET_IFNAME=ib0

torchrun-launcher() {
  srun --nodes="$GANG_NNODES" --ntasks-per-node=1 \
    --output="$GANG_LOG_DIR/rank-%n.out" --open-mode=append \
    $GANG_CONTAINER torchrun --nnodes="$GANG_NNODES" --nproc-per-node="$GANG_GPUS_PER_NODE" \
      --rdzv-backend=c10d --rdzv-endpoint="$MASTER_ADDR:$MASTER_PORT" --no-python "$@"
}

mpi-launcher() {
  module load openmpi
  mpirun -n $((GANG_NNODES * GANG_GPUS_PER_NODE)) $GANG_CONTAINER "$@"
}
```

The file is read and checked with `bash -n` at every gang submission, so an update applies
to the next job; a copy goes next to the job script, so queued jobs keep the version they
were submitted with. When a pod asks for a launcher that the site file does not define,
the submission fails rather than starting the job some other way.

Probes check a pod's own command, which a launcher replaces: for a gang started by a
launcher, the containers are reported ready while the job runs.

## GPUs

`GPUFlag` sets how a pod's `nvidia.com/gpu` (or `amd.com/gpu`) request reaches Slurm:
`gres` (`--gres=gpu:N`, the default), `gpus-per-node` or `gpus`. A GPU flag set in a flavor
or in the `slurm-job.vk.io/flags` annotation takes precedence.
