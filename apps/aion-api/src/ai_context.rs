use crate::{error::ApiError, AppState};
use aion_action::{Action, ActionResult, Command};
use aion_entity::Entity;
use aion_event::Event;
use aion_observation::Observation;
use aion_relationship::Relationship;
use aion_storage::EventFilter;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub(crate) struct AiContextQuery {
    pub include_observations: Option<bool>,
    pub include_events: Option<bool>,
    pub include_commands: Option<bool>,
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize)]
pub(crate) struct AiEntityContextResponse {
    pub target_entity: Entity,
    pub outgoing_relationships: Vec<Relationship>,
    pub incoming_relationships: Vec<Relationship>,
    pub recent_observations: Vec<Observation>,
    pub recent_events: Vec<Event>,
    pub related_commands: Vec<Command>,
    pub related_actions: Vec<Action>,
    pub related_action_results: Vec<ActionResult>,
    pub raw_message_refs: Vec<Uuid>,
    pub generated_at: DateTime<Utc>,
    pub metadata: Value,
}

pub(crate) fn build_ai_entity_context(
    state: &AppState,
    entity_id: Uuid,
    query: AiContextQuery,
) -> Result<AiEntityContextResponse, ApiError> {
    let target_entity = state
        .storage
        .get_entity(state.tenant_id, entity_id)?
        .ok_or_else(ApiError::not_found)?;

    let limit = query.limit.unwrap_or(10);
    let include_observations = query.include_observations.unwrap_or(true);
    let include_events = query.include_events.unwrap_or(true);
    let include_commands = query.include_commands.unwrap_or(true);

    let outgoing_relationships =
        state
            .storage
            .list_relationships(state.tenant_id, Some(entity_id), None)?;
    let incoming_relationships =
        state
            .storage
            .list_relationships(state.tenant_id, None, Some(entity_id))?;

    let recent_observations = if include_observations {
        state.storage.query_observations(
            state.tenant_id,
            Some(entity_id),
            None,
            None,
            None,
            limit,
        )?
    } else {
        Vec::new()
    };

    let recent_events = if include_events {
        query_events_for_entity(state, entity_id, limit)?
    } else {
        Vec::new()
    };

    let related_commands = if include_commands {
        state
            .storage
            .query_commands(state.tenant_id, Some(entity_id), None)?
            .into_iter()
            .take(limit as usize)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    let mut related_actions = Vec::new();
    let mut related_action_results = Vec::new();
    if include_commands {
        for command in &related_commands {
            related_actions.extend(
                state
                    .storage
                    .query_actions(state.tenant_id, Some(command.id))?,
            );
            related_action_results.extend(state.storage.query_action_results(
                state.tenant_id,
                None,
                Some(command.id),
            )?);
        }
        related_actions.sort_by(|left, right| {
            right
                .started_at
                .cmp(&left.started_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        related_action_results.sort_by(|left, right| {
            right
                .observed_at
                .cmp(&left.observed_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        related_actions.truncate(limit as usize);
        related_action_results.truncate(limit as usize);
    }

    let mut raw_message_refs = Vec::new();
    for raw_message_id in recent_observations
        .iter()
        .filter_map(|observation| observation.raw_message_id)
        .chain(
            recent_events
                .iter()
                .filter_map(|event| event.raw_message_id),
        )
    {
        if !raw_message_refs.contains(&raw_message_id) {
            raw_message_refs.push(raw_message_id);
        }
    }

    Ok(AiEntityContextResponse {
        target_entity,
        outgoing_relationships,
        incoming_relationships,
        recent_observations,
        recent_events,
        related_commands,
        related_actions,
        related_action_results,
        raw_message_refs,
        generated_at: Utc::now(),
        metadata: json!({
            "builder": "aion:AiContextBuilder",
            "domain_agnostic": true,
            "llm_invoked": false,
            "include_observations": include_observations,
            "include_events": include_events,
            "include_commands": include_commands,
            "limit": limit
        }),
    })
}

fn query_events_for_entity(
    state: &AppState,
    entity_id: Uuid,
    limit: u32,
) -> Result<Vec<Event>, ApiError> {
    let mut events = state.storage.query_events(
        state.tenant_id,
        EventFilter {
            target_entity_id: Some(entity_id),
            ..Default::default()
        },
    )?;

    for event in state.storage.query_events(
        state.tenant_id,
        EventFilter {
            source_entity_id: Some(entity_id),
            ..Default::default()
        },
    )? {
        if !events.iter().any(|existing| existing.id == event.id) {
            events.push(event);
        }
    }

    events.sort_by(|left, right| {
        right
            .occurred_at
            .cmp(&left.occurred_at)
            .then_with(|| right.id.cmp(&left.id))
    });
    events.truncate(limit as usize);
    Ok(events)
}
