#![feature(test)]
extern crate test;
use std::alloc::Layout;
use std::collections::VecDeque;
use std::ops::Deref;
use std::sync::Arc;
use liberasurecode::{ErasureCoder, FragmentHeader};
use object_pool::Pool;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

lazy_static::lazy_static! {
    static ref DATA_BLANK : Vec<u8> = vec![0_u8; 1024*1024];
}

#[derive(Copy, Clone)]
pub struct SubChunk(*mut u8);


impl Deref for SubChunk {
    type Target = *mut u8;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

unsafe impl Sync for SubChunk {}
unsafe impl Send for SubChunk {}

pub struct PoolRow {
    split_size: usize,
    pool: Arc<Pool<SubChunk>>,
}

impl Clone for PoolRow {
    fn clone(&self) -> Self {
        Self {
            split_size: self.split_size,
            pool: self.pool.clone(),
        }
    }
}

impl PoolRow {
    fn alloc_zeroed_buffer16(split_size: usize) -> *mut u8 {
        let layout = Layout::from_size_align(split_size, 16).unwrap();
        unsafe {
            std::alloc::alloc(layout)
        }
    }
    pub fn new(pool_size: usize, split_size: usize) -> Self {
        Self {
            split_size,
            pool : Arc::new(Pool::new(pool_size,
            || SubChunk(PoolRow::alloc_zeroed_buffer16(split_size)))),
        }
    }
}

pub struct DataStorage {
    k: usize,
    n: usize,
    sub_chunk: usize,
    data: Vec<SubChunk>,
    coding: Vec<SubChunk>,
    stored: usize,
    raw_space: usize,
    cell_len: usize,
    pool_context: PoolRow,
    cell_max: usize,
}

pub struct DataStorageIter<'a> {
    data: &'a DataStorage,
    curr: usize,
}

impl Iterator for DataStorageIter<'_> {
    type Item = SubChunk;

    fn next(&mut self) -> Option<Self::Item> {
        if self.curr < self.data.k {
           let r = self.data.data.get(self.curr).cloned();
           self.curr+=1;
           return r;
        } else if self.curr < self.data.k +self.data.n  {
           let r = self.data.coding.get(self.curr - self.data.k).cloned();
           self.curr+=1;
           return r;
        }
        None
    }
}

impl Drop for DataStorage {
    fn drop(&mut self) {
        for p in &self.data {
            self.pool_context.pool.attach(*p);
        }
        for p in &self.coding {
            self.pool_context.pool.attach(*p);
        }

    }
}

impl DataStorage {

    pub fn len(&self) -> usize {
        self.cell_max + self.cell_len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_iter(&self) -> DataStorageIter {
        DataStorageIter {
            data: self,
            curr: 0,
        }
    }

    fn new(ec: &mut PoolRow, k: usize, n: usize, sub_chunk: usize) -> DataStorage {
        let cell_len: usize = std::mem::size_of::<FragmentHeader>() + sub_chunk;
        let nr_sub_matrix = ec.split_size / cell_len;
        let raw_space = nr_sub_matrix * sub_chunk * k;

        Self {
            k,
            n,
            sub_chunk,
            data: {
                let mut v = Vec::with_capacity(k);
                for _ in 0..k {
                    let reusable_buf = ec.pool.pull(|| SubChunk(PoolRow::alloc_zeroed_buffer16(ec.split_size)));
                    let (_, val) = reusable_buf.detach();
                    v.push(val);
                }
                v
            },
            coding: {
                let mut v = Vec::with_capacity(n);
                for _ in 0..n {
                    let reusable_buf = ec.pool.pull(|| SubChunk(PoolRow::alloc_zeroed_buffer16(ec.split_size)));
                    let (_, val) = reusable_buf.detach();
                    v.push(val);
                }
                v
            },
            stored: 0,
            raw_space,
            cell_len,
            pool_context: ec.clone(),
            cell_max: 0,
        }
    }

    pub fn remaining(&self) -> usize {
        self.raw_space - self.stored
    }

    fn get_xyz(&self) -> (usize, usize, usize) {

        let where_pos = self.stored / self.sub_chunk;

        let x = where_pos % self.k; // get good vector
        let y = (where_pos / self.k) * self.cell_len;



        (x, y, self.stored - where_pos*self.sub_chunk)
    }

    fn zeroed(&mut self) {
        let (curr_vector, _, offstet_in_cell) = self.get_xyz();

        if curr_vector == 0 && offstet_in_cell == 0 {
            return;
        }
        /* complete curr vector */
        let total_to_write = self.sub_chunk - offstet_in_cell + (self.k - curr_vector - 1) * self.sub_chunk;
        if total_to_write > DATA_BLANK.len() {
            let v = vec![0_u8; total_to_write];
            self.put_slice(v.as_slice());
        } else {
            self.put_slice(&DATA_BLANK.as_slice()[0..total_to_write]);
        }
    }

    pub fn protect(&mut self, x: &ErasureCoder) {
        let nr_iter = self.cell_max / self.sub_chunk;
        let k = self.k;
        (0..nr_iter)
            .into_par_iter()
            .for_each(|val| {
                let mut data_ptr = Vec::with_capacity(k);

                let offset = val * (self.sub_chunk + std::mem::size_of::<FragmentHeader>())
                    + std::mem::size_of::<FragmentHeader>();
                for ptr in &self.data {
                    unsafe {
                        data_ptr.push(ptr.0.add(offset));
                    }
                }

                let mut coding_ptr = Vec::with_capacity(self.n);

                for ptr in &self.coding {
                    unsafe {
                        coding_ptr.push(ptr.0.add(offset));
                    }
                }
                x.encode_chunk(data_ptr.as_slice(), coding_ptr.as_slice(),
                self.sub_chunk, self.stored);
            });
    }

    pub fn put_slice (&mut self, src: &[u8]) -> usize {
        let mut to_store = src.len();

        let remaining = self.remaining();
        if to_store > remaining {
            to_store = remaining;
        }

        let mut written : usize= 0;

        while to_store != 0 {
            let (vector_nr, start_cell, offset_in_cell) = self.get_xyz();
            let v = self.data.get(vector_nr).unwrap();

            if vector_nr == 0 && start_cell > self.cell_max {
                self.cell_max = start_cell;
            }

            let curr_write = {
                let mut max = self.sub_chunk - offset_in_cell;
                if to_store < max {
                    max = to_store
                }
                max
            };
            let true_offset = start_cell + offset_in_cell + std::mem::size_of::<FragmentHeader>();
            let sub_src = (&src[written..written + curr_write]).as_ptr();
            unsafe {
                std::ptr::copy_nonoverlapping(
                    sub_src,v.0.add(true_offset), curr_write
                );
            }
            written += curr_write;
            to_store -= curr_write;
            self.stored += curr_write;
        }

        written
    }
}

pub struct DataStorageVec {
    list: VecDeque<DataStorage>,
    pool: PoolRow,
    k: usize,
    n: usize,
    sub_len: usize,
}

impl DataStorageVec {
    pub fn len(&self) -> usize {
        self.list.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn new(pool: &mut PoolRow, k: usize, n: usize, sub_len: usize) -> Self {
        Self {
            list: VecDeque::new(),
            pool: pool.clone(),
            k,
            n,
            sub_len,
        }
    }

    pub fn put_data (&mut self, data: &[u8]) -> usize {
        let mut to_write = data.len();
        let mut written = 0;
        while to_write != 0 {
            let mut p = self.list.back_mut();
            if p.is_none() || p.as_ref().unwrap().remaining() == 0 {
                self.list.push_back(DataStorage::new(&mut self.pool, self.k, self.n, self.sub_len));
                p = self.list.back_mut();
            }
            if p.is_none() {
                return written;
            }
            let r = p.unwrap().put_slice(&data[written..]);
            written += r;
            to_write -= r;
        }
        written
    }

    pub fn get_data(&mut self) -> Option<DataStorage> {
        let r = self.list.pop_front();
        if r.is_none() {
            return None;
        }
        let mut d = r.unwrap();
        if d.remaining() != 0 {
            d.zeroed();
        }
        Some(d)
    }

    pub fn get_data_complete(&mut self) -> Option<DataStorage> {
        let r = self.list.front_mut();
        r.as_ref()?;
        let d = r.unwrap();
        if d.remaining() == 0 {
            return self.get_data();
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use liberasurecode::ErasureCoder;
    use rayon::iter::{IntoParallelIterator, ParallelIterator};
    use crate::{DataStorageVec, PoolRow};
    use super::*;
    use test::Bencher;


    static SPLIT_SIZE: usize = 4*1024;

    #[test]
    fn it_works() {

        let mut ctx = PoolRow::new(4, SPLIT_SIZE);
        let er = ErasureCoder::new(NonZeroUsize::new(3).unwrap(),
                                   NonZeroUsize::new(1).unwrap()).unwrap();
        for _ in 0..10
        {
            //let mut storage = DataStorage::new(&mut ctx, 3, 1, 512);
            let mut storage = DataStorageVec::new(&mut ctx, 3, 1, 512);

            let data = (0..32768).map(|i| 'a' as u8 + (i % 24) as u8).collect::<Vec<u8>>();

            let r = storage.put_data(data.as_slice());

            //storage.zeroed();

            //storage.protect(&er);
            println!("written is {}", r);
            println!("nr_vec = {}", storage.list.len());

            while let Some(mut n) = storage.get_data() {
                n.protect(&er);
                (0..128).into_par_iter().for_each(|_| {});
            }
        }

        println!("pool len {}", ctx.pool.len());
    }

    #[bench]
    fn bench_put_protect(b: &mut Bencher) {
        let mut ctx = PoolRow::new(4, SPLIT_SIZE);
        let er = ErasureCoder::new(NonZeroUsize::new(3).unwrap(),
                                   NonZeroUsize::new(1).unwrap()).unwrap();
        let data = (0..32768).map(|i| 'a' as u8 + (i % 24) as u8).collect::<Vec<u8>>();
        b.iter(|| {
            let mut storage = DataStorageVec::new(&mut ctx, 3, 1, 512);
            storage.put_data(data.as_slice());
            while let Some(mut n) = storage.get_data() {
                n.protect(&er);
            }
        });
    }
}
