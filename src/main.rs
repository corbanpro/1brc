use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

const READ_FILE_PATH: &str = "../measurements.txt";
const CHUNK_SIZE: u64 = 16 * 1024 * 1024;
const CHUNK_EXCESS: u64 = 64;

#[derive(Clone, Copy, Debug)]
struct CityResult {
    sum: f64,
    count: f64,
    min: f64,
    max: f64,
}

impl CityResult {
    fn from(temp: f64) -> CityResult {
        CityResult {
            sum: temp,
            count: 1.0,
            min: temp,
            max: temp,
        }
    }
    fn update(&mut self, temp: f64) {
        self.sum += temp;
        self.count += 1.0;
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
    }
    fn merge(self, other: Self) -> Self {
        CityResult {
            sum: self.sum + other.sum,
            count: self.count + other.count,
            min: self.min.min(other.min),
            max: self.max.max(other.max),
        }
    }
}
type Hasher = std::collections::hash_map::RandomState;
type GenericMap<K, V> = std::collections::HashMap<K, V, Hasher>;
type BorrowedMap<'a> = GenericMap<&'a [u8], CityResult>;

macro_rules! new_map {
    ($t:ty) => {{
        let hasher = Hasher::default();
        <$t>::with_hasher(hasher)
    }};
}

fn get_aligned_buffer<'a>(file: &File, offset: u64, mut buffer: &'a mut [u8]) -> &'a [u8] {
    let metadata = file.metadata().unwrap();
    let file_size = metadata.size();

    if offset > file_size {
        return &[];
    };

    let buffer_size = buffer.len().min((file_size - offset) as usize);
    buffer = &mut buffer[..buffer_size];

    let read_from;
    let mut head;

    if offset == 0 {
        head = 0;
        read_from = 0;
    } else {
        head = CHUNK_EXCESS as usize;
        read_from = offset - CHUNK_EXCESS;
    }

    file.read_exact_at(buffer, read_from).unwrap();

    while head > 0 {
        if buffer[head - 1] == b'\n' {
            break;
        }
        head -= 1
    }

    let mut tail = buffer.len() - 1;

    while buffer[tail] != b'\n' {
        tail -= 1;
    }

    &buffer[head..=tail]
}

fn process_buffer(
    file: &File,
    thread_offset: u64,
    thread_result: &mut Arc<Mutex<HashMap<String, CityResult>>>,
    thread_buffer: &mut [u8],
) {
    let aligned_buffer = get_aligned_buffer(file, thread_offset, thread_buffer);
    let mut map = new_map!(BorrowedMap);

    for line in aligned_buffer
        .split(|&b| b == b'\n')
        .filter(|line| !line.is_empty())
    {
        let split_point = line
            .iter()
            .enumerate()
            .find_map(|(index, &b)| (b == b';').then_some(index))
            .unwrap();
        let city = &line[..split_point];
        let temp = &line[split_point + 1..];
        let temp = std::str::from_utf8(temp).unwrap();
        let temp: f64 = temp.parse().unwrap();
        map.entry(city)
            .and_modify(|city_result| city_result.update(temp))
            .or_insert_with(|| CityResult::from(temp));
    }

    let mut outer = thread_result.lock().unwrap();
    for (city, city_result) in map.into_iter() {
        let city = std::str::from_utf8(city).unwrap();
        outer
            .entry(city.to_string())
            .and_modify(|outer_records| *outer_records = outer_records.merge(city_result))
            .or_insert(city_result);
    }
}

fn distribute_work(file: &File) -> HashMap<String, CityResult> {
    let metadata = file.metadata().unwrap();
    let file_size = metadata.size();

    let offset = Arc::new(AtomicU64::new(0));
    let result = Arc::new(Mutex::new(HashMap::new()));

    thread::scope(|scope| {
        for _ in 0..thread::available_parallelism().map(Into::into).unwrap_or(1) {
            let offset = offset.clone();
            let mut thread_result = result.clone();
            scope.spawn(move || {
                let mut thread_buffer = vec![0; (CHUNK_SIZE + CHUNK_EXCESS) as usize];
                loop {
                    let thread_offset = offset.fetch_add(CHUNK_SIZE, Ordering::SeqCst);
                    if thread_offset > file_size {
                        break;
                    }
                    process_buffer(file, thread_offset, &mut thread_result, &mut thread_buffer)
                }
            });
        }
    });

    Arc::into_inner(result).unwrap().into_inner().unwrap()
}

fn main() {
    let file = File::open(READ_FILE_PATH).unwrap();
    let results = distribute_work(&file);

    let mut keys = results.keys().collect::<Vec<_>>();
    keys.sort_unstable();

    for key in keys {
        let record = results[key];
        let min = record.min;
        let avg = record.sum / record.count;
        let max = record.max;
        println!("{key}: {:.1}/{:.1}/{:.1}", min, avg, max);
    }
}
