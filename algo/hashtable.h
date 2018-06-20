/*
    Copyright 2018. simba wei

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef HASHTABLE_H
#define HASHTABLE_H

#include "../common/lock.h"
#include <cassert>

class HashTable
{
    public:
        void init(unsigned int nbuckets, unsigned int bucksize, unsigned int tuplesize);
        void destroy();

        void* allocate(unsigned int offset);

        inline void set_start(unsigned long long start_value)
        {
            start_value_ = start_value;
        }

        inline void set_end(unsigned long long end_value)
        {
            end_value_ = end_value;
        }

        inline unsigned long long get_start()
        {
            return start_value_;
        }

        inline unsigned long long get_end()
        {
            return end_value_;
        }

        inline unsigned int get_bucket_num()
        {
            return nbuckets_;
        }

        inline void* atomic_allocate(unsigned int offset)
        {
            void* ret;
            lock_[offset].lock();
            ret = allocate(offset);
            lock_[offset].unlock();
            return ret;
        }

        class Iterator
        {
            friend class HashTable;
            public:
                Iterator() : bucksize_(0), tuplesize_(0), cur_(0), next_(0), free_(0){ }
                Iterator(unsigned int bucksize, unsigned int tuplesize);

                inline void* read_next()
                {
                    void* ret;

                    if(cur_ < free_)
                    {
                        ret = cur_;
                        cur_ = ((char*)cur_) + tuplesize_;
                        return ret;
                    }
                    else if(next_ != 0)
                    {
                        ret = next_;
                        cur_ = ((char*)next_)+tuplesize_;
                        free_ = *(void**)((char*)next_ + bucksize_);
                        next_ = *(void**)((char*)next_ + bucksize_ + sizeof(void*));

                        return ret < free_ ? ret : 0;
                    }
                    else
                    {
                        return 0;
                    }
                }
            private:
                void* cur_;
                void* free_;
                void* next_;
                const unsigned int bucksize_;
                const unsigned int tuplesize_;
        };

        Iterator create_iterator();


        inline void place_iterator(Iterator& it, unsigned int offset)
        {
            void* start = bucket_[offset];
            it.cur_ = start;
            it.free_ = *(void**)((char*)start + bucksize_);
            it.next_ = *(void**)((char*)start + bucksize_ + sizeof(void*));
        }

        inline void prefetch(unsigned int offset)
        {
#ifdef __x86_64__
            __asm__ __volatile__ ("prefetcht0 %0" :: "m" (*(unsigned long long*) bucket_[offset & (nbuckets_-1)]));
#endif
        }

    private:
        Lock* lock_;
        void** bucket_;

        unsigned int tuplesize_;
        unsigned int bucksize_;   ///<for data
        unsigned int nbuckets_;
        unsigned long long start_value_;
        unsigned long long end_value_;

};

#endif // HASHTABLE_H
