use term_macros::*;
use rayon::prelude::*;
use nohash_hasher::{IntSet, IntMap};
use bumpalo_herd::Herd;
use unicode_segmentation::UnicodeSegmentation;
use std::io::prelude::*;
use std::hash::Hasher;
use std::hash::Hash;
use std::io::stdout;
use std::io::BufWriter;
// needs to be inverse to the proportion actually found in natural text
// we don't need to perform division? no, we do. fuck it.
// idea: msgpack serialisation to avoid holding it in memory?
type Count = f64;
type WordId = u64;
// note: always use type aliases. they make code clearer and allow for easier refactoring.

struct FrequencyMap {
    pub frqs: IntMap<WordId, Count>,
}

impl FrequencyMap {
    pub fn new(freqs: &Vec<&mut ProcessedSentence>) -> Self {
        let mut frqs = IntMap::default();
        let mut total = 0.0;
        freqs.iter().for_each(|v| {
            v.words.iter().for_each(|w| {
                if let Some(entry) = frqs.get_mut(w) {
                    *entry += 1.0;
                } else {
                    frqs.insert(*w, 1.0);
                }
                total += 1.0;
            })
        });
        for (_, val) in frqs.iter_mut() {
            *val = total / (*val as f64).cbrt();
        }
        Self {
            frqs
        }
    }
}



// just need
// - read file into memory as string - 1.5m
// - segment into words - 2 mins
// - hash words - 30 seconds
// - read the words into memory - 1m
// - scoring function - 10m
// - sort by score - 3m

fn hash_str(s: &str) -> u64 {
    let mut h = fnv::FnvHasher::with_key(0);
    s.hash(&mut h);
    h.finish()
}

fn score(words_to_score: &IntSet<u64>, desired_words: &IntSet<u64>, table: &FrequencyMap) -> f64 {
    words_to_score.iter()
        .map(|w| {
            if let Some(f) = table.frqs.get(w) {
                return if desired_words.contains(w) {
                    *f
                } else {
                    -1.0 * *f
                }
            }
            0.0
        })
        .sum::<f64>()
}

type Filename = String;

fn open(name: &Filename) -> IntSet<u64> {
    let mut string = String::new();
    let _ = std::fs::File::open(&name).unwrap().read_to_string(&mut string);
    string.split("\n").map(|s| hash_str(s)).collect()
}

// ok: new approach. memmap file, find new lines, create bump-allocated vec of:

struct Sentence {
    slice_start: usize,
    slice_end: usize,
    line_number: usize
}

struct ProcessedSentence {
    words: IntSet<u64>,
    line_number: usize
}

fn proc_sentence(s1: &Sentence, map: &[u8]) -> ProcessedSentence {
    let slice = &map[s1.slice_start..s1.slice_end];
    let as_str = std::str::from_utf8(slice).unwrap();
    ProcessedSentence {
        words: as_str.unicode_words()
            .map(|w| w.to_lowercase())
            .map(|w| hash_str(&w))
            .collect(),
        line_number: s1.line_number
    }
}

fn main() {
    tool! {
        args:
            - wordlist: Filename;
            - sentences: Filename;
        ;

        body: || {
            // might be off by one line number count
            let herd = Herd::new();

            let words = open(&wordlist);
            let map = mmap!(sentences);

            let newline_indices: Vec<_> = map.par_iter()
                .enumerate()
                .filter(|(_, b)| **b == b'\n')
                .map(|(i, _)| i)
                .collect();

            let sentences = (1..newline_indices.len())
                .map(|line_number| {
                    let slice_start = newline_indices[line_number-1] + 1;
                    let slice_end = newline_indices[line_number];
                    Sentence {
                        slice_start,
                        slice_end,
                        line_number: line_number-1
                    }
                })
                .collect::<Vec<_>>();

            let processed_sentences = sentences
                .par_iter()
                .map_init(|| herd.get(), |bump, s| {
                    bump.alloc(proc_sentence(s, &map[..]))
                })
                .collect::<Vec<_>>();

            let frequencies = FrequencyMap::new(&processed_sentences);

            let mut scores = processed_sentences.par_iter()
                .map(|s| {
                    (s.line_number, score(&s.words, &words, &frequencies))
                })
                .collect::<Vec<_>>();

            scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

            let mut stdout = BufWriter::new(stdout().lock());

            for (line_no, _score) in scores {
                let start = sentences[line_no].slice_start;
                let end = sentences[line_no].slice_end;
                stdout.write_all(&map[start..end]).unwrap();
                stdout.write_all(b"\n").unwrap();
            }
        }
    }
}
