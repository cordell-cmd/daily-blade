# Time and Aging Plan for Daily Blade

## Goals
- Give the world a consistent calendar pace independent of real-world days.
- Track character lifecycle state over time: first seen age, current age, health trajectory, lineage, and death.
- Keep existing characters usable by retrofitting temporal fields from current data.
- Feed temporal context into story generation and haiku output without forcing every tale to focus on age.

## Core Time Model
Use publication issues as the simulation clock.

- `issue_index`: Monotonic issue number from archive chronology.
- `world_days_per_issue`: Configurable pace multiplier.
- `world_day_index`: `issue_index * world_days_per_issue`.
- `world_year`: `floor(world_day_index / 365)`.

Recommended default:
- `world_days_per_issue = 10`

Implication:
- 1 real-world year (365 issues) ~= 10 in-world years.
- The world ages fast enough for visible generational change while preserving continuity.

## Character Temporal Schema
Add a `temporal` object to character-like entities in codex/lore outputs.

```json
{
  "temporal": {
    "first_recorded_date": "2026-03-06",
    "first_recorded_issue_index": 10,
    "age_first_recorded_years": 28,
    "birth_issue_index_est": -1012,
    "current_issue_index": 63,
    "current_age_years": 29.45,
    "life_stage": "adult",
    "alive": true,
    "deceased_date": null,
    "deceased_issue_index": null,
    "health": {
      "baseline_vitality": 0.74,
      "chronic_conditions": ["shadowburn scarring"],
      "active_conditions": [],
      "frailty": 0.21,
      "resilience": 0.66
    },
    "wisdom": {
      "experience_points": 14.2,
      "wisdom_score": 0.61,
      "temperament_shift": "steadier under pressure"
    },
    "lineage": {
      "parents": [],
      "children": [],
      "ancestors": []
    }
  }
}
```

## Lifecycle Mechanics
### Aging
- `current_age_years = age_first_recorded_years + (current_issue_index - first_recorded_issue_index) * world_days_per_issue / 365`
- `life_stage` bands:
  - `child`: < 13
  - `youth`: 13-19
  - `adult`: 20-44
  - `mature`: 45-64
  - `elder`: 65+

### Mortality and Death
Use weighted risk, not deterministic cutoffs.

Mortality hazard:
- Base hazard by life stage.
- Increase by active severe conditions and crisis exposure.
- Reduce by resilience, healing events, and social support indicators.

When death occurs:
- `alive = false`
- set `deceased_date` and `deceased_issue_index`
- preserve historical references and ancestry links.

### Disease and Conditions
Condition model per character:
- `acute`: plague, wound fever, poisoning
- `chronic`: old war injury, wasting cough, memory fracture
- `age-linked`: joint decline, failing sight, brittle bones

Transitions:
- `new -> active -> recovering/resolved` OR `new -> active -> chronic`
- Chance of worsening with age/frailty and conflict intensity.

### Births and Lineage
For paired/connected adult characters, allow child events based on:
- age band and health,
- relationship continuity,
- story pressure (peace windows or aftermath windows).

When child appears:
- Add character with lineage references to parents.
- Add ancestor graph links for descendants over time.

## Retrofit Strategy for Existing Characters
Do not wait for new stories. Backfill now.

1. Build issue timeline from `archive/index.json` and `stories.json`.
2. For each character with `first_date`, map to `first_recorded_issue_index`.
3. Seed `age_first_recorded_years` using role priors (configurable table), then clamp by status clues.
4. Compute `current_age_years` with current issue index.
5. Initialize baseline health/wisdom from role + arc exposure (story appearances count, event intensity exposure).
6. Mark clearly inferred fields with provenance metadata:
   - `temporal_inference`: `"estimated" | "story_explicit" | "mixed"`

## Haiku Integration
Inject temporal context into generation prompts as compact facts.

Prompt-side variables per character mention:
- `age_now`, `life_stage`, `alive`, `health_signals`, `wisdom_score`, `recent_temporal_change`.

Guidelines:
- Young voices: kinetic, searching, future-facing imagery.
- Elder voices: memory-dense, consequence-aware imagery.
- Illness/recovery: body-state motifs, fragility vs persistence.
- Grief/ancestry: inherited burden, names, tools, songs.

Keep this subtle:
- At most 1-2 temporal motifs per haiku unless the story is explicitly about aging/illness/death.

## Migration Plan (Phased)
### Phase 1 (safe foundation)
- Add temporal config constants.
- Add issue-index utilities.
- Add backfill script to write temporal fields to lore/codex/characters.
- Surface read-only temporal badges in UI.

### Phase 2 (simulation)
- Add per-issue temporal update pass in generation pipeline.
- Apply health/condition transitions.
- Apply mortality and birth events.

### Phase 3 (narrative integration)
- Thread temporal context into story and haiku prompts.
- Add optional timeline filters in codex UI.

## Recommended First Build Order
1. Implement issue-index + world clock utility module.
2. Implement `backfill_character_temporal.py`.
3. Add `temporal` badges in codex cards (age, life stage, alive/deceased).
4. Add prompt context block for haiku generation.
5. Run one migration pass and audit sample characters for plausibility.

## Guardrails
- Never rewrite explicit canonical ages from text.
- Preserve deceased characters in codex; do not delete.
- Keep inferred values deterministic and reproducible.
- Include an opt-out config to disable lifecycle simulation for debugging.
