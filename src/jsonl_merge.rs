use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Dedup key extracted from a JSONL line.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
enum DedupKey {
    EntryId(String),
    Id(String),
    ContentHash(String),
}

/// A parsed JSONL line together with its dedup key and optional timestamp.
struct ParsedLine {
    key: DedupKey,
    ts: Option<String>,
    raw: String,
}

fn parse_line(line: &str) -> Option<ParsedLine> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    let value: Value = serde_json::from_str(trimmed).ok()?;

    let key = if let Some(entry_id) = value.get("entry_id").and_then(|v| v.as_str()) {
        DedupKey::EntryId(entry_id.to_string())
    } else if let Some(id) = value.get("id").and_then(|v| v.as_str()) {
        DedupKey::Id(id.to_string())
    } else {
        let mut hasher = Sha256::new();
        hasher.update(trimmed.as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        DedupKey::ContentHash(hash)
    };

    let ts = value.get("ts").and_then(|v| v.as_str()).map(|s| s.to_string());

    Some(ParsedLine {
        key,
        ts,
        raw: trimmed.to_string(),
    })
}

/// Merge two JSONL byte slices using union + dedup + ts-based stable sort.
///
/// Dedup priority:
///   1. `entry_id` field (UUID)
///   2. `id` field (e.g. K-001)
///   3. SHA-256 content hash of the entire line (fallback)
///
/// When duplicate keys exist, the line with the latest `ts` wins.
/// Lines without a `ts` field are placed at the front after sorting.
/// Lines that fail JSON parsing or are empty are silently skipped.
pub fn merge_jsonl(local: &[u8], remote: &[u8]) -> Vec<u8> {
    let local_str = String::from_utf8_lossy(local);
    let remote_str = String::from_utf8_lossy(remote);

    // Collect all parsed lines from both sources.
    // Remote lines come after local so that when keys collide with equal ts,
    // the remote (later) entry wins via the insert-overwrite below.
    let all_lines = local_str
        .lines()
        .chain(remote_str.lines())
        .filter_map(parse_line);

    // Dedup: keep only the entry with the latest ts for each key.
    let mut map: HashMap<DedupKey, ParsedLine> = HashMap::new();
    for line in all_lines {
        match map.get(&line.key) {
            Some(existing) => {
                // If the new line has a >= ts (or existing has no ts), replace.
                let dominated = match (&existing.ts, &line.ts) {
                    (None, _) => true,
                    (_, None) => false,
                    (Some(a), Some(b)) => b >= a,
                };
                if dominated {
                    map.insert(line.key.clone(), line);
                }
            }
            None => {
                map.insert(line.key.clone(), line);
            }
        }
    }

    // Stable sort by ts (None first, then ascending).
    let mut results: Vec<ParsedLine> = map.into_values().collect();
    results.sort_by(|a, b| {
        let a_ts = a.ts.as_deref().unwrap_or("");
        let b_ts = b.ts.as_deref().unwrap_or("");
        a_ts.cmp(b_ts)
    });

    // Serialize back to JSONL.
    let mut output = String::new();
    for line in &results {
        output.push_str(&line.raw);
        output.push('\n');
    }
    output.into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// local 3줄 + remote 2줄 (겹침 1줄) → 결과 4줄
    #[test]
    fn merge_preserves_all() {
        let local = br#"{"id":"K-001","ts":"2024-01-01","data":"a"}
{"id":"K-002","ts":"2024-01-02","data":"b"}
{"id":"K-003","ts":"2024-01-03","data":"c"}
"#;
        let remote = br#"{"id":"K-002","ts":"2024-01-02","data":"b"}
{"id":"K-004","ts":"2024-01-04","data":"d"}
"#;
        let result = merge_jsonl(local, remote);
        let result_str = String::from_utf8(result).unwrap();
        let lines: Vec<&str> = result_str.lines().collect();
        assert_eq!(lines.len(), 4, "expected 4 lines, got: {:?}", lines);
    }

    /// 동일 내용 라인 (id/entry_id 없음) → content hash로 dedup → 1줄
    #[test]
    fn merge_dedup_by_content_hash() {
        let local = br#"{"ts":"2024-01-01","data":"same"}
"#;
        let remote = br#"{"ts":"2024-01-01","data":"same"}
"#;
        let result = merge_jsonl(local, remote);
        let result_str = String::from_utf8(result).unwrap();
        let lines: Vec<&str> = result_str.lines().collect();
        assert_eq!(lines.len(), 1, "expected 1 line, got: {:?}", lines);
    }

    /// 같은 id, 다른 내용 → 최신 ts인 1줄만 남음
    #[test]
    fn merge_dedup_by_id() {
        let local = br#"{"id":"K-001","ts":"2024-01-01","data":"old"}
"#;
        let remote = br#"{"id":"K-001","ts":"2024-06-15","data":"new"}
"#;
        let result = merge_jsonl(local, remote);
        let result_str = String::from_utf8(result).unwrap();
        let lines: Vec<&str> = result_str.lines().collect();
        assert_eq!(lines.len(), 1, "expected 1 line, got: {:?}", lines);
        assert!(
            result_str.contains("\"data\":\"new\""),
            "expected newer line to be preserved"
        );
    }

    /// 무작위 순서 입력 → ts 오름차순 출력
    #[test]
    fn merge_sorted_by_ts() {
        let local = br#"{"id":"A","ts":"2024-03-01","data":"third"}
{"id":"B","ts":"2024-01-01","data":"first"}
"#;
        let remote = br#"{"id":"C","ts":"2024-02-01","data":"second"}
"#;
        let result = merge_jsonl(local, remote);
        let result_str = String::from_utf8(result).unwrap();
        let lines: Vec<&str> = result_str.lines().collect();
        assert_eq!(lines.len(), 3);

        // Parse ts from each output line and verify ascending order.
        let ts_owned: Vec<String> = lines
            .iter()
            .map(|l| {
                let v: Value = serde_json::from_str(l).unwrap();
                v.get("ts").unwrap().as_str().unwrap().to_string()
            })
            .collect();
        assert!(
            ts_owned.windows(2).all(|w| w[0] <= w[1]),
            "timestamps not in ascending order: {:?}",
            ts_owned
        );
    }

    /// 빈 local + 3줄 remote → 3줄 결과
    #[test]
    fn merge_empty_inputs() {
        let local = b"";
        let remote = br#"{"id":"X","ts":"2024-01-01","data":"a"}
{"id":"Y","ts":"2024-02-01","data":"b"}
{"id":"Z","ts":"2024-03-01","data":"c"}
"#;
        let result = merge_jsonl(local, remote);
        let result_str = String::from_utf8(result).unwrap();
        let lines: Vec<&str> = result_str.lines().collect();
        assert_eq!(lines.len(), 3, "expected 3 lines, got: {:?}", lines);
    }
}
