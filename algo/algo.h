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

#ifndef ALGO_H
#define ALGO_H

#include <libconfig.h++>
#include <vector>
#include "../common/table.h"
#include "../common/hash.h"
#include "../common/lock.h"
#include "hashtable.h"
#include "../common/cache.h"


class BaseAlgo
{
    public:
        BaseAlgo(const libconfig::Setting& cfg);
        virtual ~BaseAlgo() { }

        virtual void init(
            Schema* schema1, vector<unsigned int> select1, unsigned int jattr1,
            Schema* schema2, vector<unsigned int> select2, unsigned int jattr2,
            unsigned int selectivity, unsigned long long cond_s,unsigned long long cond_e);

        virtual void destroy();

        virtual void build(PageCursor* t, ht_node* node) = 0;
        virtual PageCursor* probe(PageCursor* t, ht_node* node) = 0;
    protected:
        Schema* s1_, * s2_, * sout_, * sbuild_;
        vector<unsigned int> sel1_, sel2_;
        unsigned int ja1_, ja2_, size_, selectivity_;
        unsigned long long cond_s_, cond_e_;
};


class HashBase : public BaseAlgo
{
    public:
        HashBase(const libconfig::Setting& cfg);
        virtual ~HashBase();
        virtual void init(
           Schema* schema1, vector<unsigned int> select1, unsigned int jattr1,
           Schema* schema2, vector<unsigned int> select2, unsigned int jattr2,
           unsigned int selectivity, unsigned long long cond_s, unsigned long long cond_e);
        virtual void destroy();
        virtual void build(PageCursor* t, ht_node* node) = 0;
        virtual PageCursor* probe(PageCursor* t, ht_node* node) = 0;
    protected:
        //HashTable hashtable_;
        int outputsize_;
};

class StoreCopy : public HashBase
{
    public:
        StoreCopy(const libconfig::Setting& cfg) : HashBase(cfg) {}
        virtual ~StoreCopy() {}

        virtual void init(
           Schema* schema1, vector<unsigned int> select1, unsigned int jattr1,
           Schema* schema2, vector<unsigned int> select2, unsigned int jattr2,
           unsigned int selectivity, unsigned long long cond_s, unsigned long long cond_e);
        virtual void destroy();

        virtual void build(PageCursor* t, ht_node* node) = 0;
        virtual PageCursor* probe(PageCursor* t, ht_node* node) = 0;

    protected:
        void buildCursor(PageCursor* t, ht_node* node,bool atomic);

        WriteTable* probeCursor(PageCursor* t, ht_node* node ,bool atomic, WriteTable* ret = NULL);

    private:
        template <bool atomic>
        void realbuildCursor(PageCursor* t, ht_node* node);

        template <bool atomic>
        WriteTable* realprobeCursor(PageCursor* t, ht_node* node, WriteTable* ret = NULL);
};

class StorePointer : public HashBase
{
    public:
        StorePointer(const libconfig::Setting& cfg) : HashBase(cfg) {}
        virtual ~StorePointer() {}

        virtual void init(
            Schema* schema1, vector<unsigned int> select1, unsigned int jattr1,
            Schema* schema2, vector<unsigned int> select2, unsigned int jattr2,
            unsigned int selectivity, unsigned long long cond_s, unsigned long long cond_e);
        virtual void destroy();

        virtual void build(PageCursor* t, ht_node* node) = 0;
        virtual PageCursor* probe(PageCursor* t, ht_node* node) = 0;

    protected:
        void buildCursor(PageCursor* t, ht_node* node ,bool atomic);

        WriteTable* probeCursor(PageCursor* t, ht_node* node, bool atomic, WriteTable* ret = NULL);

    private:
        template <bool atomic>
        void realbuildCursor(PageCursor* t, ht_node* node);

        template <bool atomic>
        WriteTable* realprobeCursor(PageCursor* t, ht_node* node ,WriteTable* ret = NULL);
};

template <typename Super>
class BuildPhase : public Super
{
    public:
        BuildPhase(const libconfig::Setting& cfg) : Super(cfg) {}
        virtual ~BuildPhase() {}
        virtual void build(PageCursor* t, ht_node* node);
};


template <typename Super>
class ProbePhase : public Super
{
    public:
        ProbePhase(const libconfig::Setting& cfg) : Super(cfg) {}
        virtual ~ProbePhase() {}
        virtual PageCursor* probe(PageCursor* t, ht_node* node);
};


#include "build.inl"
#include "probe.inl"

#endif // ALGO_H
