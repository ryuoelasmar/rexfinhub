# Project Development Instructions

This project uses the dev-orchestrator system for multi-agent development.

## Custom Agents Available

Select from the agent dropdown in VS Code Chat:
- **Orchestrator**: Plans multi-step work and coordinates agents
- **Implementer**: Implements code changes directly
- **Reviewer**: Reviews code for correctness and security

## CLI Commands (run in terminal)

```
orchestrate plan "description"   Plan parallel task breakdown
orchestrate run                  Spawn parallel Claude Code workers
orchestrate status               Check agent progress
orchestrate merge                Merge completed work
```

## Coordination Files

- `CLAUDE.md` - Project context (auto-loaded by Claude Code workers)
- `.agents/BOUNDARIES.md` - File ownership for active tasks
- `.agents/tasks/` - Active task assignments
- `.worktrees/` - Isolated workspaces for parallel features (auto-managed)

## Rules

- All code agents use Claude Opus 4.6
- Workers only edit files they own (see `.agents/BOUNDARIES.md`)
- Worktrees are inside `.worktrees/` (never external directories)
- For simple changes, use the Implementer agent directly
- For parallel multi-feature work, use the Orchestrator + CLI
