/*
    Copyright 2018, simba wei.

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

#include "table.h"
#include "loader.h"
#include <glob.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include "exceptions.h"
#include "stdlib.h"
#include <assert.h>



void Table::init(Schema *s, unsigned int size)
{
    schema_ = s;
    data_head_ = 0;
    cur_ = 0;
}

void Table::close()
{
    LinkedTupleBuffer* t = data_head_;
    while(t)
    {
        t = data_head_->get_next();
        delete data_head_;
        data_head_ = t;
    }
    reset();
}


vector<PageCursor*> Table::split(int nthreads)
{
    vector<PageCursor*> ret = vector<PageCursor*>();

    for (int i=0; i<nthreads; ++i) {
        Table* pt = new WriteTable();
        pt->init(schema_, 0);
        ret.push_back(pt);
    }

    int curthr = 0;
    Bucket* pb = get_root();
    Bucket* pbnew = NULL;
    Table* curtbl = NULL;
    while (pb) {
        pbnew = pb->get_next();

        curtbl = (Table*) ret[curthr];
        pb->set_next(curtbl->data_head_);
        curtbl->data_head_ = pb;

        curthr = (curthr + 1) % nthreads;
        pb = pbnew;
    }

    for (int i=0; i<nthreads; ++i) {
        ret[i]->reset();
    }

    data_head_ = NULL;
    cur_ = NULL;

    return ret;
}

void WriteTable::append(const char **data, unsigned int count)
{
    unsigned int s = schema_->get_tuple_size();
    if(!last_->can_store(s))
    {
        LinkedTupleBuffer* tmp = new LinkedTupleBuffer(size_,s);
        last_->set_next(tmp);
        last_ = tmp;
    }
    if(schema_->columns() == count)
    {
        void* target = last_->allocate_tuple();
        schema_->parse_tuple(target,data);
    }
    else
    {
        std::cout<<"ERROR COUNT!"<<endl;
        throw NotYetImplemented();
    }
}

void WriteTable::append(const vector<string> &input)
{
    unsigned int s = schema_->get_tuple_size();
    if(! last_->can_store(s))
    {
        LinkedTupleBuffer* tmp = new LinkedTupleBuffer(size_,s);
        last_->set_next(tmp);
        last_ = tmp;
    }
    void* target = last_->allocate_tuple();
    schema_->parse_tuple(target,input);
}

void WriteTable::append(const void * const src)
{
    unsigned int s = schema_->get_tuple_size();

    if(!last_->can_store(s))
    {
        LinkedTupleBuffer* tmp = new LinkedTupleBuffer(size_,s);
        last_->set_next(tmp);
        last_ = tmp;
    }

    void* target = last_->allocate_tuple();
    if(target != NULL)
    {
        schema_->copy_tuple(target,src);
    }
}

void WriteTable::concatenate(const WriteTable& table)
{
    if(schema_->get_tuple_size() == table.schema_->get_tuple_size())
    {
        if (data_head_ == 0)
        {
            data_head_ = table.data_head_;
        }
        last_->set_next(table.data_head_);
        last_ = table.last_;
    }
}

void WriteTable::init(Schema* s, unsigned int size)
{
    Table::init(s, size);

    this->size_=size;
    data_head_ = new LinkedTupleBuffer(size, s->get_tuple_size());
    last_ = data_head_;
    cur_ = data_head_;
}

void AtomicWriteTable::append(const void* const src) {
    unsigned int s = schema_->get_tuple_size();
    lock_.lock();
    if (!last_->can_store(s))
    {
        // create a new bucket
        LinkedTupleBuffer* tmp = new LinkedTupleBuffer(size_, s);
        // link it as the next bucket of last
        last_->set_next(tmp);
        // make last point to the new bucket
        last_ = tmp;
    }

    void* target = last_->allocate_tuple();
    lock_.unlock();
    schema_->copy_tuple(target, src);
}

void AtomicWriteTable::append(const vector<string>& input) {
    unsigned int s = schema_->get_tuple_size();
    lock_.lock();
    if (!last_->can_store(s))
    {
        // create a new bucket
        LinkedTupleBuffer* tmp = new LinkedTupleBuffer(size_, s);
        // link it as the next bucket of last
        last_->set_next(tmp);
        // make last point to the new bucket
        last_ = tmp;
    }

    void* target = last_->allocate_tuple();
    lock_.unlock();
    schema_->parse_tuple(target, input);
}

/**
 * Returns end of chain starting at \a head, NULL if \a head is NULL.
 */
LinkedTupleBuffer* find_chain_end(LinkedTupleBuffer* head)
{
    LinkedTupleBuffer* prev = NULL;
    while (head)
    {
        prev = head;
        head = head->get_next();
    }
    return prev;
}

Table::LoadErrorT WriteTable::load(const string& filepattern,
        const string& separators)
{
    Loader loader(separators[0]);
    loader.load(filepattern, *this);
    return LOAD_OK;
}


void Table::print_table()
{
    LinkedTupleBuffer* ret = data_head_;

    while(ret)
    {
        unsigned int idx = 0;
        void* tuple = ret->get_tuple_offset(idx);
        while (tuple)
        {
            cout<< *reinterpret_cast<long long*>(tuple)<< " ";
            cout<< *reinterpret_cast<long long*>(tuple+sizeof(long long))<< " ";
            tuple = ret->get_tuple_offset((++idx));
        }
        //cout<< endl;
        ret = ret->get_next();
    }
}

void WriteTable::non_temporal_append16(const void * const src)
{
    unsigned int s = schema_->get_tuple_size();
    assert(s==16);

    if (!last_->can_store(s)) {
        // create a new bucket
        LinkedTupleBuffer* tmp = new LinkedTupleBuffer(size_, s);
        // link it as the next bucket of last
        last_->set_next(tmp);
        // make last point to the new bucket
        last_ = tmp;
    }

    void* target = last_->allocate_tuple();
    assert(target!=NULL);
#ifdef __x86_64__
    __asm__ __volatile__ (
            "	movntiq %q2, 0(%4)	\n"
            "	movntiq %q3, 8(%4)	\n"
            : "=m" (*(unsigned long long*)target), "=m" (*(unsigned long long*)((char*)target+8))
            : "r" (*(unsigned long long*)((char*)src)), "r" (*((unsigned long long*)(((char*)src)+8))), "r" (target)
            : "memory");
#else
#warning MOVNTI not known for this architecture
    schema_->copy_tuple(target, src);
#endif
}























