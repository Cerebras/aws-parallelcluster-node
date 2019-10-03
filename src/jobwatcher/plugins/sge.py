import logging

from common.schedulers.sge_commands import (
    SGE_BUSY_STATES,
    SGE_HOLD_STATE,
    SGE_ORPHANED_STATE,
    get_compute_nodes_info,
    get_pending_jobs_info,
)

log = logging.getLogger(__name__)


# CEREBRAS MODIFICATION
# =====================
# Upstream's _get_required_slots() implementation completely ignores idle slots
# in the existing compute nodes.  We have modified this to track the existing
# available slots and attempt to fit job slots into the existing slots though
# simple greedy matching.
def _get_required_slots(instance_properties, max_size):
    """Compute the total number of slots required by pending jobs."""

    # First construct a list of available slots per node
    avail_slots = []
    nodes = get_compute_nodes_info()
    for node in nodes.values():
        avail_slots.append(instance_properties.get("slots") - int(node.slots_used) - int(node.slots_reserved))
    avail_slots.sort()
    logging.info("slots before job match: %s", str(avail_slots))

    max_cluster_slots = max_size * instance_properties.get("slots")
    pending_jobs = get_pending_jobs_info(max_slots_filter=max_cluster_slots, skip_if_state=SGE_HOLD_STATE)
    slots = 0
    for job in pending_jobs:
        found_slot = False
        for i in range(len(avail_slots)):
            if job.slots <= avail_slots[i]:
                avail_slots[i] -= job.slots
                found_slot = True
                break
        if not found_slot:
            slots += job.slots
    logging.info("slots after job match: %s", str(avail_slots))

    return slots


# get nodes requested from pending jobs
def get_required_nodes(instance_properties, max_size):
    required_slots = _get_required_slots(instance_properties, max_size)
    vcpus = instance_properties.get("slots")
    return -(-required_slots // vcpus)


def get_busy_nodes():
    """
    Count nodes that have at least 1 job running or have a state that makes them unusable for jobs submission.
    """
    nodes = get_compute_nodes_info()
    busy_nodes = 0
    for node in nodes.values():
        if (
            any(busy_state in node.state for busy_state in SGE_BUSY_STATES)
            or int(node.slots_used) > 0
            or int(node.slots_reserved) > 0
        ):
            if SGE_ORPHANED_STATE in node.state:
                logging.info(
                    "Skipping host %s since in orphaned state, hence not in ASG. "
                    "Host will disappear when assigned jobs are deleted.",
                    node.name,
                )
            else:
                busy_nodes += 1

    return busy_nodes
