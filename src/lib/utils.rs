use std::collections::VecDeque;
use std::future::Future;

// optimization might be possible
pub fn bytes2i64(bytes: &[u8]) -> Option<i64> {
    if bytes.len() == 0 {
        return None;
    }
    let mut result = 0i64;
    let mut invalid = true;
    let mut negative = false;
    for i in 0..bytes.len() {
        if i == 0 && bytes[i] == b'-' {
            negative = true;
            continue;
        }
        if bytes[i] >= b'0' && bytes[i] <= b'9' {
            invalid = false;
            result = result * 10 + (bytes[i] - b'0') as i64;
        } else {
            break;
        }
    }
    if invalid {
        return None;
    }
    if negative {
        result = 0 - result;
    }
    Some(result)
}

pub fn bytes2u64(bytes: &[u8]) -> Option<u64> {
    bytes2i64(bytes).filter(|x| *x > 0).map(|x| x as u64)
}

pub fn merge_sorted_vec(a: &mut VecDeque<u64>, b: &mut VecDeque<u64>) -> VecDeque<u64> {
    let mut new_value = VecDeque::new();
    loop {
        let v = match (a.pop_front(), b.pop_front()) {
            (None, None) => break,
            (Some(v), None) => v,
            (None, Some(v)) => v,
            (Some(v), Some(vv)) => {
                if v >= vv {
                    a.push_front(v);
                    vv
                } else {
                    b.push_front(vv);
                    v
                }
            }
        };
        if let Some(p) = new_value.back() {
            if *p >= v {
                continue;
            }
        }
        new_value.push_back(v);
    }
    new_value
}

pub fn run_async_in_current_thread<F>(f: F)
where
    F: Future + 'static,
    F::Output: 'static,
{
    let rt = tokio::runtime::Runtime::new().unwrap();
    let ls = tokio::task::LocalSet::new();
    ls.block_on(&rt, async move {
        let _ = tokio::task::spawn_local(f).await;
    });
}
