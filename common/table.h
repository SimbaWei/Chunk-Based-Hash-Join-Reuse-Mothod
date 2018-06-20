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

#ifndef TABLE_H
#define TABLE_H

#include<string>
#include<vector>
#include "schema.h"
#include "page.h"
#include "lock.h"
#include "exceptions.h"

class TupleBufferCursor;
typedef TupleBufferCursor PageCursor;

class TupleBufferCursor
{
    public:
        /**
         * Returns the next non-requested page, or NULL if no next page exists.
         * Not necessarily thread-safe!!!
         */
        virtual TupleBuffer* read_next() { return atomic_read_next(); }

        /**
         * Returns the next non-requested page, or NULL if no next page exists.
         * A synchronized version of readNext().
         */
        virtual TupleBuffer* atomic_read_next() = 0;

        /** Return \ref Schema object for all pages. */
        virtual Schema* schema() = 0;

        /**
         * Resets the reading point to the start of the table.
         */
        virtual void reset() { }

        /**
         * Closes the cursor.
         */
        virtual void close() { }

        virtual vector<PageCursor*> split(int nthreads) = 0;

        virtual ~TupleBufferCursor() { }

};

/*
 * container for a linked list of LinkedTupleBuffer.
 */
class Table : public TupleBufferCursor
{
    public:
        Table() : schema_(NULL), data_head_(NULL), cur_(NULL) { }
        virtual ~Table() { }

        enum LoadErrorT
        {
            LOAD_OK = 0
        };

        virtual LoadErrorT load(const string& filepattern,
                const string& separators) = 0;

        virtual void init(Schema* s, unsigned int size);

        inline LinkedTupleBuffer* get_root()
        {
            return data_head_;
        }

        /**
         * Returns the next non-requested bucket, or NULL if no next bucket exists.
         * Not thread-safe!!!
         */
        inline LinkedTupleBuffer* read_next()
        {
            LinkedTupleBuffer* ret = cur_;
            if (cur_)
                cur_ = cur_->get_next();
            return ret;
        }

        /**
         * Returns the next non-requested bucket, or NULL if no next bucket exists.
         * A synchronized version of readNext().
         */
        inline LinkedTupleBuffer* atomic_read_next()
        {
            LinkedTupleBuffer* oldval;
            LinkedTupleBuffer* newval;

            newval = cur_;

            do
            {
                if (newval == NULL)
                    return NULL;

                oldval = newval;
                newval = oldval->get_next();
                newval = (LinkedTupleBuffer*)atomic_compare_and_swap((void**)&cur_, oldval, newval);

            } while (newval != oldval);

            return newval;
        }

        /**
         * Resets the reading point to the start of the table.
         */
        inline void reset()
        {
            cur_ = data_head_;
        }

        /**
         * Close the table, ie. destroy all data associated with it.
         * Not closing the table will result in a memory leak.
         */
        virtual void close();

        /** Return associated \ref Schema object. */
        Schema* schema()
        {
            return schema_;
        }

        vector<PageCursor*> split(int nthreads);

        void print_table();
    protected:
        Schema* schema_;
        LinkedTupleBuffer* data_head_;
        /* volatile */ LinkedTupleBuffer* cur_;
};

class WriteTable : public Table {
    public:
        WriteTable() : last_(NULL), size_(0) { }
        virtual ~WriteTable() { }

        virtual void whatever()  { }
        void init(Schema* s, unsigned int size);

        /**
         * Loads a single text file, where each line is a tuple and each field
         * is separated by any character in the \a separators string.
         */
        LoadErrorT load(const string& filepattern, const string& separators);

        /**
         * Appends the \a input at the end of this table, creating new
         * buckets as necessary.
         */
        virtual void append(const vector<string>& input);
        virtual void append(const char** data, unsigned int count);
        virtual void append(const void * const src);
        void non_temporal_append16(const void* const src);

        /**
         * Appends the table to this table.
         * PRECONDITION: Caller must check that schemas are same.
         */
        void concatenate(const WriteTable& table);

    protected:
        LinkedTupleBuffer* last_;
        unsigned int size_;
};

class AtomicWriteTable : public WriteTable {
    public:
        virtual ~AtomicWriteTable() { }

        virtual void append(const vector<string>& input);
        virtual void append(const void* const src);
    private:
        Lock lock_;
};

class FakeTable : public PageCursor {
    public:
        FakeTable(Schema* s)
            : sch_(s), first_time_(false)
        { }

        void place(unsigned int pagesz, unsigned int tuplesz, void* data)
        {
            fake_page_.place(pagesz, tuplesz, data);
            first_time_ = true;
        }

        virtual Page* atomicReadNext()
        {
            if (first_time_) {
                first_time_ = false;
                return &fake_page_;
            } else {
                return NULL;
            }
        }

        virtual Schema* schema() { return sch_; }

        virtual void reset() { first_time_ = true; }

        virtual vector<PageCursor*> split(int nthreads)
        {
            cout<<"not implement!"<<endl;
            throw NotYetImplemented();
        }

    private:
        FakePage fake_page_;
        Schema* sch_;
        bool first_time_;

};








#endif // TABLE_H
