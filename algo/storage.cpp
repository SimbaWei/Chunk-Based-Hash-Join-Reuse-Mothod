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

#include "algo.h"
#ifdef VERBOSE
#include <iostream>
#include <iomanip>
using namespace std;
#endif

void StoreCopy::init(Schema *schema1, vector<unsigned int> select1, unsigned int jattr1,
  Schema *schema2, vector<unsigned int> select2, unsigned int jattr2, unsigned int selectivity, unsigned long long cond_s, unsigned long long cond_e)
{
    //unsigned int nbucket = 512;
    HashBase::init(schema1,select1,jattr1,schema2,select2,jattr2,selectivity,cond_s,cond_e);
    //hashtable_.init(nbucket, size_, sbuild_->get_tuple_size());
}


void StoreCopy::destroy()
{
    HashBase::destroy();
    //hashtable_.destroy();
}

void StorePointer::destroy()
{
    delete sbuild_;

    //hashtable_.destroy();
}

void StoreCopy::buildCursor(PageCursor *t, ht_node *node, bool atomic)
{
    if(atomic)
        realbuildCursor<true>(t,node);
    else
        realbuildCursor<false>(t,node);

}

template<bool atomic>
void StoreCopy::realbuildCursor(PageCursor* t, ht_node *node)
{
    int i = 0;
    void* tup;
    Page* b;
    Schema*s  = t->schema();
    unsigned int curbuc;
    unsigned int buckpos;
    unsigned int nbuckets = node->hashtable_->get_bucket_num();
    if(node->init_)
    {
        cout << "It is new node"<<flush<<endl;
    }
    while (b = (atomic ? t->atomic_read_next() : t->read_next()))
    {
        i = 0;
        while(tup = b->get_tuple_offset(i++))
        {
            // find hash table to append
            unsigned long long value1 = s->as_long(tup,ja1_);
            if(value1 < cond_s_ || value1 > cond_e_)
            {
                continue;
            }
            if(value1 >= node->start_value_ && value1 <= node->end_value_ && !(node->init_))
            {
                //cout<<"should be here"<<endl;
                continue;
            }
            //cout<< "value1 is " << value1 <<endl;
            curbuc = murmurhash2(&value1,s->get_column_type_size(ja1_),0);
            buckpos = curbuc % nbuckets;
            //cout<< "cal value1 is " << curbuc << "buckpos is " << buckpos <<"\t" <<"curbuc is "<< curbuc<<endl;
            void* target = atomic ? node->hashtable_->atomic_allocate(buckpos) :
                                    node->hashtable_->allocate(buckpos);

#ifdef VERBOSE
        cout << "Adding tuple with key "
            << setfill('0') << setw(7) << s->as_long(tup, ja1_)
            << " to bucket " << setfill('0') << setw(4) << curbuc << endl;
#endif
            sbuild_->write_data(target, 0, s->calc_offset(tup, ja1_));
            for(unsigned int j=0; j<sel1_.size(); ++j)
            {
                sbuild_->write_data(target,                 // dest
                         j+1,                               // col in output
                         s->calc_offset(tup,sel1_[j]));     // src for this col
            }
        }
    }
    node->start_value_ = cond_s_ < node->start_value_ ? cond_s_ : node->start_value_;
    node->end_value_ = cond_e_ > node->end_value_ ? cond_e_ : node->end_value_;
    node->init_ = false;
    //node->hashtable_->print();
    cout << "cond_s:"<<cond_s_<<" cond_e:"<<cond_e_<<endl;
    cout << "start:"<<node->start_value_<<" end:"<<node->end_value_<<endl;
    cout << "Finishing build hashtable!, cache hashtable:["<<node->start_value_<<","<<node->end_value_<<"]" << endl;
}

WriteTable* StoreCopy::probeCursor(PageCursor *t, ht_node *node, bool atomic, WriteTable *ret)
{
    if (atomic)
        return realprobeCursor<true>(t,node, ret);
    return realprobeCursor<false>(t,node, ret);
}


template<bool atomic>
WriteTable* StoreCopy::realprobeCursor(PageCursor* t, ht_node *node, WriteTable* ret)
{
    if(ret == NULL)
    {
        ret = new WriteTable();
        ret->init(sout_,outputsize_);
    }

    char tmp[sout_->get_tuple_size()];
    void* tup1;
    void* tup2;

    Page* b2;
    unsigned int curbuc, i, buckpos;
    unsigned nbuckets = node->hashtable_->get_bucket_num();

    HashTable::Iterator it = node->hashtable_->create_iterator();

    while(b2 = (atomic ? t->atomic_read_next() : t->read_next()))
    {
#ifdef VERBOSE
        cout << "Working on page " << b2 << endl;
#endif
        i = 0;
        while(tup2 = b2->get_tuple_offset(i++))
        {
#ifdef VERBOSE
            cout << "Joining tuple " << b2 << ":"
                << setfill('0') << setw(6) << i
                << " having key " << s2->as_long(tup2, ja2_) << endl;
#endif
            unsigned long long value = s2_->as_long(tup2,ja2_);
            if(value < cond_s_ || value > cond_e_)
            {
                continue;
            }
            curbuc = murmurhash2(&value, s2_->get_column_type_size(ja2_),0);
#ifdef VERBOSE
            cout << "\twith bucket " << setfill('0') << setw(6) << curbuc << endl;
#endif
            buckpos = curbuc % nbuckets;
            //cout<< "Joined value is " << value <<"\t" << "cur is "<< curbuc << "\t" <<"buckpos is "<<buckpos<<"\t"<<"size is "<<sizeof(s2_->get_column_type_size(ja2_))<<endl;
            node->hashtable_->place_iterator(it,buckpos);

#ifdef PREFETCH
#warning Only works for 16-byte tuples!
            node->hashtable_->prefetch(murmurhash2(s2_->as_pointer(reinterpret_cast<void*>((char*)tup2+32),ja2_), sizeof(s2_,ja2_),0));
            node->hashtable_->prefetch(murmurhash2(s2_->as_pointer(reinterpret_cast<void*>((char*)tup2+32),ja2_), sizeof(s2_,ja2_),0));
#endif
//            if(it.read_next() == NULL)
//            {
//                cout <<"Empty!" << flush<<endl;
//            }
            while(tup1 = it.read_next())
            {
                if(sbuild_->as_long(tup1,0) != value)
                {
                    continue;
                }
                //cout<< "Joined value is " << value <<"\t" << "cur is "<< curbuc << "\t" <<"buckpos is "<<buckpos<<endl;
#if defined(OUTPUT_ASSEMBLE)
                // copy payload of first tuple to destination
                if (s1_->get_tuple_size())
                    s1_->copy_tuple(tmp, sbuild_->calc_offset(tup1,1));

                // copy each column to destination
                for (unsigned int j=0; j<sel2_.size(); ++j)
                    sout_->write_data(tmp,		// dest
                            s1_->columns()+j,	// col in output
                            s2_->calc_offset(tup2, sel2_[j]));	// src for this col
#if defined(OUTPUT_WRITE_NORMAL)
                ret->append(tmp);
#elif defined(OUTPUT_WRITE_NT)
                ret->non_temporal_append16(tmp);
#endif
#endif

#if !defined(OUTPUT_AGGREGATE) && !defined(OUTPUT_ASSEMBLE)
                __asm__ __volatile__ ("nop");
#endif
            }
        }
    }
    return ret;
}

void StorePointer::init(Schema *schema1, vector<unsigned int> select1, unsigned int jattr1,
  Schema *schema2, vector<unsigned int> select2, unsigned int jattr2, unsigned int selectivity,
  unsigned long long cond_s, unsigned long long cond_e)
{
    HashBase::init(schema1, select1, jattr1, schema2, select2, jattr2,selectivity,cond_s,cond_e);

    // override sbuild, add s1
    s1_ = schema1;
    delete sbuild_;

    // generate build schema
    sbuild_ = new Schema();
    sbuild_->add(schema1->get(ja1_));
    sbuild_->add(CT_POINTER);

    // create hashtable with new build schema
    //hashtable_.init(512, size_, sbuild_->get_tuple_size());
}


void StorePointer::buildCursor(PageCursor *t, ht_node* node ,bool atomic)
{
    if(atomic)
        realbuildCursor<true>(t,node);
    else
        realbuildCursor<false>(t,node);
}

template <bool atomic>
void StorePointer::realbuildCursor(PageCursor* t, ht_node* node)
{
    int i = 0;
    void* tup;
    Page* b;
    Schema* s = t->schema();
    unsigned int curbuc;
    while(b = (atomic ? t->atomic_read_next() : t->read_next()))
    {
        i = 0;
        while(tup = b->get_tuple_offset(i++)) {
            // find hash table to append
            curbuc = murmurhash2(s->as_pointer(tup,ja1_),s->get_column_type_size(ja1_),0);
            void* target = atomic ?
                node->hashtable_->atomic_allocate(curbuc) :
                node->hashtable_->allocate(curbuc);

#ifdef VERBOSE
        cout << "Adding tuple with key "
            << setfill('0') << setw(7) << s->asLong(tup, ja1)
            << " to bucket " << setfill('0') << setw(4) << curbuc << endl;
#endif

            sbuild_->write_data(target, 0, s->calc_offset(tup, ja1_));
            sbuild_->write_data(target, 1, &tup);

        }
    }
}



WriteTable* StorePointer::probeCursor(PageCursor *t, ht_node *node, bool atomic, WriteTable *ret)
{
    if (atomic)
        return realprobeCursor<true>(t, node, ret);
    return realprobeCursor<false>(t, node, ret);
}

template <bool atomic>
WriteTable* StorePointer::realprobeCursor(PageCursor* t, ht_node *node , WriteTable* ret)
{
    if (ret == NULL) {
        ret = new WriteTable();
        ret->init(sout_, outputsize_);
    }

    char tmp[sout_->get_tuple_size()];
    void* tup1;
    void* tup2;
    Page* b2;
    unsigned int curbuc, i;

    HashTable::Iterator it = node->hashtable_->create_iterator();

    while (b2 = (atomic ? t->atomic_read_next() : t->read_next())) {
        i = 0;
        while (tup2 = b2->get_tuple_offset(i++)) {
            curbuc = murmurhash2(s2_->as_pointer(tup2,ja2_), sizeof(s2_->get_column_type_size(ja2_)),0);
            node->hashtable_->place_iterator(it, curbuc);

#ifdef PREFETCH
#warning Only works for 16-byte tuples!
            node->hashtable_->prefetch(murmurhash2(s2_->as_pointer(reinterpret_cast<void*>((char*)tup2+32),ja2_), sizeof(s2_,ja2_),0));
            node->hashtable_->prefetch(murmurhash2(s2_->as_pointer(reinterpret_cast<void*>((char*)tup2+32),ja2_), sizeof(s2_,ja2_),0));
#endif

            while (tup1 = it.read_next()) {
                if (sbuild_->as_long(tup1,0) != s2_->as_long(tup2,ja2_) ) {
                    continue;
                }

#if defined(OUTPUT_ASSEMBLE)
                void* realtup1 = sbuild_->as_pointer(tup1, 1);
                // copy each column to destination
                for (unsigned int j=0; j<sel1_.size(); ++j)
                    sout_->write_data(tmp,		// dest
                            j,		// col in output
                            s1_->calc_offset(realtup1, sel1_[j]));	// src for this col
                for (unsigned int j=0; j<sel2_.size(); ++j)
                    sout_->write_data(tmp,		// dest
                            sel1_.size()+j,	// col in output
                            s2_->calc_offset(tup2, sel2_[j]));	// src for this col
#if defined(OUTPUT_WRITE_NORMAL)
                ret->append(tmp);
#elif defined(OUTPUT_WRITE_NT)
                ret->non_temporal_append16(tmp);
#endif
#endif


#if !defined(OUTPUT_AGGREGATE) && !defined(OUTPUT_ASSEMBLE)
                __asm__ __volatile__ ("nop");
#endif

            }
        }
    }
    return ret;
}




































