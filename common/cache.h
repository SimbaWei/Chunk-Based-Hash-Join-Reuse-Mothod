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

#ifndef CACHE_H
#define CACHE_H

#include "../algo/hashtable.h"
#include <iostream>

struct ht_node
{
    unsigned long long start_value_;
    unsigned long long end_value_;
    HashTable hashtable_;
    ht_node* next_;
};

class ReuseCache
{
    public:
        ReuseCache(max_cache_size)
            :max_cache_size_(max_cache_size)
        {
            cache_head_ = NULL;
            curr_cache_size_ = 0;
        }

        ht_node* insert(
           unsigned long long start,
           unsigned long long end,
           unsigned int bucksize,
           unsigned int tuplesize);

        ht_node* get_reusable_ht(unsigned long long start, unsigned long long end);

        void add_cache(unsigned long long addtional_cache);

        void destroy();

        void garbage_collection(); ///< LRU algorithm.

    private:
        ht_node* cache_head_;
        unsigned long long  max_cache_size_;
        unsigned long long  curr_cache_size_;

};

#endif // CACHE_H
