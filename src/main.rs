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
const CARD_LIFETIME: Duration = Duration::from_millis(3_400);
const EXIT_ANIMATION: Duration = Duration::from_millis(950);
const STREAM_STEP: Duration = Duration::from_millis(50);
const MAX_CARDS: usize = 11;

const PRESETS: &[Preset] = &[
    Preset {
        slug: "cyberpunk",
        name: "Cyberpunk Terminal",
        tag: "ROOT",
        message: "green phosphor packet breaches the grid",
        hue: 146,
    },
    Preset {
        slug: "broadside",
        name: "Renaissance Broadside",
        tag: "PRESS",
        message: "inked heraldry unfolds from the printing press",
        hue: 34,
    },
    Preset {
        slug: "bulletin",
        name: "Paper Bulletin",
        tag: "PINNED",
        message: "fresh paper note slaps onto the cork wall",
        hue: 52,
    },
    Preset {
        slug: "combo",
        name: "Crazy Combo",
        tag: "COMBO",
        message: "impact chain detonates into bonus score confetti",
        hue: 24,
    },
    Preset {
        slug: "neon",
        name: "Neon Arcade",
        tag: "INSERT",
        message: "hot pink cabinet lights punch through the dark",
        hue: 315,
    },
    Preset {
        slug: "hologram",
        name: "Hologram Glass",
        tag: "SCAN",
        message: "transparent signal resolves from a blue light sweep",
        hue: 196,
    },
    Preset {
        slug: "glitch",
        name: "Glitch Siren",
        tag: "ERROR",
        message: "misaligned red cyan frame snaps back together",
        hue: 6,
    },
    Preset {
        slug: "blueprint",
        name: "Blueprint Pulse",
        tag: "DRAFT",
        message: "technical linework draws itself in electric ink",
        hue: 220,
    },
    Preset {
        slug: "sticker",
        name: "Sticker Slap",
        tag: "SLAP",
        message: "die cut label bounces with a glossy peel",
        hue: 282,
    },
    Preset {
        slug: "toxic",
        name: "Toxic Radar",
        tag: "PING",
        message: "acid radar sweep flashes over warning stripes",
        hue: 116,
    },
    Preset {
        slug: "noir",
        name: "Noir Flashbulb",
        tag: "FLASH",
        message: "silver headline appears through film grain",
        hue: 260,
    },
    Preset {
        slug: "prism",
        name: "OKLab Prism",
        tag: "PRISM",
        message: "wide gamut gradient blooms then settles into glass",
        hue: 178,
    },
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
        yield Ok(stats_event(0, 0, 0, "warming up"));

        let mut sequence = 0_u64;
        let mut latest_preset = "warming up";
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
                latest_preset = card.preset.name;
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

                outgoing.push(stats_event(sequence, live_count, leaving_count, latest_preset));
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

fn stats_event(total: u64, live: usize, leaving: usize, preset: &str) -> Event {
    PatchSignals::new(format!(
        r#"{{"total": {total}, "live": {live}, "leaving": {leaving}, "preset": "{}"}}"#,
        escape_attr(preset),
    ))
    .into()
}

#[derive(Clone, Copy, Debug)]
struct Preset {
    slug: &'static str,
    name: &'static str,
    tag: &'static str,
    message: &'static str,
    hue: u16,
}

#[derive(Clone, Debug)]
struct Card {
    id: u64,
    born_at: Instant,
    remove_at: Option<Instant>,
    preset: Preset,
    tilt: i16,
    drift_x: i16,
    drift_y: i16,
}

impl Card {
    fn new(id: u64, born_at: Instant) -> Self {
        let index = (id.saturating_sub(1) as usize) % PRESETS.len();
        let preset = PRESETS[index];
        let jitter = id as i16;

        Self {
            id,
            born_at,
            remove_at: None,
            preset,
            tilt: ((jitter * 7) % 9) - 4,
            drift_x: ((jitter * 13) % 31) - 15,
            drift_y: ((jitter * 17) % 25) - 12,
        }
    }

    fn render(&self, leaving: bool) -> String {
        let state_class = if leaving { "leaving" } else { "entering" };
        let status = if leaving { "leaving" } else { self.preset.tag };
        let words = render_words(self.preset.message);

        format!(
            r#"<article id="msg-{id}" class="toast preset-{slug} {state_class}" data-preset="{name}" style="--hue: {hue}; --tilt: {tilt}deg; --drift-x: {drift_x}px; --drift-y: {drift_y}px;">
    <span class="toast-burst" aria-hidden="true"></span>
    <span class="toast-index">#{id:03}</span>
    <span class="toast-copy">
        <span class="toast-preset">{name}</span>
        <span class="toast-message">{words}</span>
    </span>
    <span class="toast-status">{status}</span>
</article>"#,
            id = self.id,
            slug = self.preset.slug,
            state_class = state_class,
            name = self.preset.name,
            hue = self.preset.hue,
            tilt = self.tilt,
            drift_x = self.drift_x,
            drift_y = self.drift_y,
            words = words,
            status = status,
        )
    }
}

fn render_words(message: &str) -> String {
    message
        .split_whitespace()
        .enumerate()
        .map(|(index, word)| {
            format!(
                r#"<span class="toast-word" style="--i: {index};">{}</span>"#,
                escape_attr(word)
            )
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn escape_attr(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
