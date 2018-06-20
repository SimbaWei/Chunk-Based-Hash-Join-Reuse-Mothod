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

#include "cache.h"
#include "exceptions.h"

using namespace std;


ht_node* ReuseCache::insert(
   unsigned long long start,
   unsigned long long end,
   unsigned int bucksize,
   unsigned int tuplesize)
{
    if(NULL != cache_head_)
    {
        ht_node* node  =(ht_node*)new ht_node();
        node->start_value_ = start;
        node->end_value_ = end;
        node->hashtable_.init((end-start)/2,bucksize,tuplesize);
        node->next_ = cache_head_;
        cache_head_ = node;

        curr_cache_size_ = curr_cache_size_ + end - start;

        return cache_head_;
    }
    else
    {
        cache_head_ = (ht_node*)new ht_node();
        cache_head_->start_value_ = start;
        cache_head_->end_value_ = end;
        cache_head_->hashtable_.init((end-start)/2,bucksize,tuplesize);
        cache_head_->next_ =NULL;
    }

    return cache_head_;
}


ht_node* ReuseCache::get_reusable_ht(unsigned long long start, unsigned long long end)
{
    ht_node* ret = NULL;
    ht_node* target = NULL;
    ret = cache_head_;
    double ratio;
    while(NULL != ret)
    {
        if(ret->start_value_ >= end || ret->end_value_ <= start)
        {

        }
        else
        {
            unsigned long long interval1 = end - start;
            unsigned long long interval2 = (ret->end_value_ >= end ? ret->end_value_ : end)
                    - (ret->start_value_ <= start ? ret->start_value_ : start);
            double tmp_ratio = (double)(interval1/interval2);
            if(ratio < tmp_ratio)
            {
                target = ret;
                ratio = tmp_ratio;
            }

        }
        ret =  ret->next_;
    }
    if(ratio >= 0.6)
    {
        return target;
    }
    else
    {
        return NULL;
    }
}


void ReuseCache::add_cache(unsigned long long addtional_cache)
{
    curr_cache_size_ = curr_cache_size_+addtional_cache;
}

void ReuseCache::destroy()
{
    ht_node* node = NULL;
    while (cache_head_ != NULL) {
        node = cache_head_;
        cache_head_ = cache_head_->next_;
        delete node;
    }
}

void ReuseCache::garbage_collection()
{
    if(curr_cache_size_ > max_cache_size_)
    {
       unsigned long long size = 0;
       ht_node* node = cache_head_;
       ht_node* pre_node = cache_head_;
       while(node)
       {
           size = size + node->end_value_ - node->start_value_;
           if(size > max_cache_size_)
           {
               ht_node* p = node;
               while(p)
               {
                   ht_node* tmp = p;
                   p = p->next_;
                   cout<<"Collect HashTable:["<<tmp->start_value_<<","<<tmp->end_value_<<"]"<<flush<<endl;
                   delete tmp;
               }
               pre_node->next_  = NULL;
               break;
           }
           pre_node = node;
           node = node->next_;
       }
       cout<< "Finish Garbage Collection!"<< flush<< endl;
    }
    else
    {
        cout << "Max Cache Size is "<<max_cache_size_<<"\t" << "Current Cache Size is "<<curr_cache_size_<<"\t"<<"Quit GC"<< flush<<endl;
    }
}
















