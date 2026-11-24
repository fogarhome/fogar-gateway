class OrchestratorError(RuntimeError):
    """The orchestrator failed a turn. Its message is always a stable code
    (`orchestrator_error`, `turn_timeout`, ...), never raw exception/OpenClaw text, so
    callers may relay it. Any other RuntimeError is NOT safe to relay (#228)."""
