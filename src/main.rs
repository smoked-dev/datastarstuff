use std::{collections::VecDeque, convert::Infallible, time::Duration};

use async_stream::stream;
use axum::{
    Router,
    response::{
        Html, IntoResponse,
        sse::{Event, KeepAlive, Sse},
    },
    routing::get,
};
use datastar::prelude::*;
use futures_core::Stream;
use tokio::time::{Instant, MissedTickBehavior, interval};

const INDEX_HTML: &str = include_str!("../public/index.html");
const STYLES_CSS: &str = include_str!("../public/styles.css");

const SPAWN_EVERY: Duration = Duration::from_millis(500);
const CARD_LIFETIME: Duration = Duration::from_millis(2_500);
const EXIT_ANIMATION: Duration = Duration::from_millis(700);
const STREAM_STEP: Duration = Duration::from_millis(50);
const MAX_CARDS: usize = 9;

const MESSAGES: &[&str] = &[
    "patched a fresh fragment into the stream",
    "morph pass kept the panel state intact",
    "SSE packet landed cleanly",
    "CSS entry curve is doing the heavy lift",
    "old fragments are queued for a graceful exit",
    "Datastar appended this box without client JS",
    "layout stayed stable through another update",
    "the backend is still driving the DOM",
    "exit class added before removal",
    "two updates per second, steady and calm",
    "a little depth, no framework ceremony",
    "fresh text box joined the stack",
];

#[tokio::main]
async fn main() {
    let app = Router::new()
        .route("/", get(index))
        .route("/styles.css", get(styles))
        .route("/stream", get(stream_updates));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .expect("bind demo server");

    println!("Datastar live box demo: http://{}", listener.local_addr().unwrap());

    axum::serve(listener, app)
        .await
        .expect("serve demo application");
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn styles() -> impl IntoResponse {
    ([("content-type", "text/css; charset=utf-8")], STYLES_CSS)
}

async fn stream_updates() -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let updates = stream! {
        yield Ok(reset_stream_event());
        yield Ok(stats_event(0, 0, 0));

        let mut sequence = 0_u64;
        let mut active_cards = VecDeque::<Card>::new();
        let mut next_spawn = Instant::now();
        let mut tick = interval(STREAM_STEP);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tick.tick().await;

            let now = Instant::now();
            let mut outgoing = Vec::new();

            while now >= next_spawn {
                sequence += 1;
                let card = Card::new(sequence, now);
                outgoing.push(append_card_event(&card));
                active_cards.push_back(card);
                next_spawn += SPAWN_EVERY;
            }

            while active_cards.len() > MAX_CARDS {
                if let Some(card) = active_cards
                    .iter_mut()
                    .find(|card| card.remove_at.is_none())
                {
                    card.remove_at = Some(now + EXIT_ANIMATION);
                    outgoing.push(update_card_event(card, true));
                } else {
                    break;
                }
            }

            for card in &mut active_cards {
                if card.remove_at.is_none() && now.duration_since(card.born_at) >= CARD_LIFETIME {
                    card.remove_at = Some(now + EXIT_ANIMATION);
                    outgoing.push(update_card_event(card, true));
                }
            }

            while active_cards
                .front()
                .and_then(|card| card.remove_at)
                .is_some_and(|remove_at| remove_at <= now)
            {
                if let Some(card) = active_cards.pop_front() {
                    outgoing.push(remove_card_event(card.id));
                }
            }

            if !outgoing.is_empty() {
                let live_count = active_cards
                    .iter()
                    .filter(|card| card.remove_at.is_none())
                    .count();
                let leaving_count = active_cards.len().saturating_sub(live_count);

                outgoing.push(stats_event(sequence, live_count, leaving_count));
            }

            for event in outgoing {
                yield Ok(event);
            }
        }
    };

    Sse::new(updates).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(5))
            .text("keep-alive"),
    )
}

fn reset_stream_event() -> Event {
    PatchElements::new(r#"<div id="stream-items" aria-live="polite" aria-atomic="false"></div>"#)
        .into()
}

fn append_card_event(card: &Card) -> Event {
    PatchElements::new(card.render(false))
        .selector("#stream-items")
        .mode(ElementPatchMode::Prepend)
        .into()
}

fn update_card_event(card: &Card, leaving: bool) -> Event {
    PatchElements::new(card.render(leaving)).into()
}

fn remove_card_event(id: u64) -> Event {
    PatchElements::new_remove(format!("#msg-{id}")).into()
}

fn stats_event(total: u64, live: usize, leaving: usize) -> Event {
    PatchSignals::new(format!(
        "{{total: {total}, live: {live}, leaving: {leaving}}}"
    ))
    .into()
}

#[derive(Clone, Debug)]
struct Card {
    id: u64,
    born_at: Instant,
    remove_at: Option<Instant>,
    tone: usize,
    message: &'static str,
}

impl Card {
    fn new(id: u64, born_at: Instant) -> Self {
        Self {
            id,
            born_at,
            remove_at: None,
            tone: (id as usize % 6) + 1,
            message: MESSAGES[id as usize % MESSAGES.len()],
        }
    }

    fn render(&self, leaving: bool) -> String {
        let class = if leaving { "toast leaving" } else { "toast" };
        let status = if leaving { "leaving" } else { "entering" };

        format!(
            r#"<article id="msg-{id}" class="{class} tone-{tone}">
    <span class="toast-index">#{id:03}</span>
    <p class="toast-message">{message}</p>
    <span class="toast-status">{status}</span>
</article>"#,
            id = self.id,
            class = class,
            tone = self.tone,
            message = self.message,
            status = status,
        )
    }
}
