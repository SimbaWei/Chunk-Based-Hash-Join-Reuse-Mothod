/*
    Copyright 2018, Simba Wei.

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

#ifndef PAGE_H
#define PAGE_H
#include "atomics.h"
class Buffer
{
    public:
        friend unsigned int page_copy(void*, const Buffer*);

        /**
         * Creates a Buffer to operate on the block starting at \a *data.
         * Class does not own the block.
         * @param data Start of block.
         * @param size Size of block.
         * @param free Free section of the block. If NULL, it is assumed that
         * the entire block is full with useful data and no further writes are
         * allowed.
         */
        Buffer(void* data, unsigned int size, void* free);

        /**
         * Allocates an empty buffer of specified size that is owned by this
         * instance.
         * \param size Buffer size in bytes.
         */
        Buffer(unsigned long size);

        ~Buffer();

        /**
         * Returns true if \a len bytes can be stored in this page.
         * Does not guarantee that a subsequent allocation will succeed, as
         * some other thread might beat us to allocate.
         * @param len Bytes to store.
         * @return True if \a len bytes can be stored in this page.
         */
        inline bool can_store(unsigned int len);

        /**
         * Returns a memory location and moves the \a free pointer
         * forward. NOT THREAD-SAFE!
         * @return Memory location good for writing up to \a len bytes, else
         * NULL.
         */
        inline void* allocate(unsigned int len);

        /**
         * Returns a memory location and atomically moves the \a free pointer
         * forward. The memory location is good for writing up to \a len
         * bytes.
         * @return Memory location good for writing up to \a len bytes, else
         * NULL.
         */
        inline void* atomic_allocate(unsigned int len);

        /**
         * Returns whether this address is valid for reading \a len bytes.
         * That is, checks if \a loc points anywhere between \a data and \a
         * data + \a maxsize - \a len.
         * @return True if address points into this page and \a len bytes can
         * be read off from it.
         */
        inline bool is_valid_address(void* loc, unsigned int len);

        /**
         * Clears contents of page.
         */
        inline void clear() { free_ = data_; }

        /**
         * Returns the capacity of this buffer.
         */
        inline const unsigned int capacity() { return maxsize_; }

        /**
         * Returns the used space of this buffer.
         */
        inline const unsigned int get_used_space();

    protected:
        /** Data segment of page. */
        void* data_;

        unsigned long maxsize_;

        /**
         * True if class owns the memory at \a *data and is responsible for its
         * deallocation, false if not.
         */
        bool owner_;

        /**
         * Marks the free section of data segment, therefore data <= free <
         * data+maxsize.
         */
        /* volatile */ void* free_;
};

class TupleBuffer : public Buffer
{
    public:
        TupleBuffer(void* data, unsigned int size, void* free, unsigned int tuplesize);

        /**
         * Creates a buffer of size \a size, holding tuples which are
         * \a tuplesize bytes each.
         * \param size Buffer size in bytes.
         * \param tuplesize Size of tuples in bytes.
         */
        TupleBuffer(unsigned long size, unsigned int tuplesize);
        ~TupleBuffer() { }

        /**
         * Returns true if a tuple can be stored in this page.
         * @return True if a tuple can be stored in this page.
         */
        inline bool can_store_tuple()
        {
            return can_store(tuplesize_);
        }

        /**
         * Returns the pointer to the start of the \a pos -th tuple,
         * or NULL if this tuple doesn't exist in this page.
         *
         * Note:
         * MemMappedTable::close() assumes that data==getTupleOffset(0).
         */
        inline void* get_tuple_offset(unsigned int pos);

        /**
         * Returns whether this address is valid, ie. points anywhere between
         * \a data and \a data + \a maxsize - \a tuplesize.
         * @return True if address points into this page.
         */
        inline bool is_valid_tuple_address(void* loc);

        /**
         * Returns a memory location and moves the \a free pointer
         * forward. NOT THREAD-SAFE!
         * @return Memory location good for writing a tuple, else NULL.
         */
        inline void* allocate_tuple();

        /**
         * Returns a memory location and atomically moves the \a free pointer
         * forward. The memory location is good for writing up to \a len
         * bytes.
         * @return Memory location good for writing a tuple, else NULL.
         */
        inline void* atomic_allocate_tuple();

        class Iterator {
            friend class TupleBuffer;

            protected:
                Iterator(TupleBuffer* p) : tupleid_(0), page_(p) { }

            public:
                Iterator& operator= (Iterator& rhs) {
                    page_ = rhs.page_;
                    tupleid_ = rhs.tupleid_;
                    return *this;
                }

                inline
                void place(TupleBuffer* p)
                {
                    page_ = p;
                    reset();
                }

                inline
                void* next()
                {
                    return page_->get_tuple_offset(tupleid_++);
                }

                inline
                void reset()
                {
                    tupleid_ = 0;
                }

            private:
                int tupleid_;
                TupleBuffer* page_;
        };

        Iterator createIterator()
        {
            return Iterator(this);
        }

    protected:
        unsigned int tuplesize_;

};

/**
 * A \a TupleBuffer with a "next" pointer.
 * @deprecated Only used by Table classes.
 */
class LinkedTupleBuffer : public TupleBuffer {
    public:

        LinkedTupleBuffer(void* data, unsigned int size, void* free,
                unsigned int tuplesize)
            : TupleBuffer(data, size, free, tuplesize), next_(0)
        { }

        /**
         * Creates a bucket of size \a size, holding tuples which are
         * \a tuplesize bytes each.
         * \param size Bucket size in bytes.
         * \param tuplesize Size of tuples in bytes.
         */
        LinkedTupleBuffer(unsigned int size, unsigned int tuplesize)
            : TupleBuffer(size, tuplesize), next_(0) { }

        /**
         * Returns a pointer to next bucket.
         * @return Next bucket, or NULL if it doesn't exist.
         */
        LinkedTupleBuffer* get_next()
        {
            return next_;
        }

        /**
         * Sets the pointer to the next bucket.
         * @param bucket Pointer to next bucket.
         */
        void set_next(LinkedTupleBuffer* const bucket)
        {
            next_ = bucket;
        }

    private:
        LinkedTupleBuffer* next_;
};

typedef TupleBuffer Page;
typedef LinkedTupleBuffer Bucket;

class FakePage : public Page {
    public:
        FakePage(unsigned int size, unsigned int tuplesz, void* existingdata)
            : Page(size, tuplesz)
        {
            delete[] reinterpret_cast<char*>(data_);
            data_ = existingdata;
            // invalidate free pointer, making page "full".
            free_ = reinterpret_cast<char*>(data_) + maxsize_;
        }

        FakePage()
            : Page(0, 0)
        {
            delete[] reinterpret_cast<char*>(data_);
            data_ = 0;	// NULL
            free_ = 0;	// NULL
        }

        void place(unsigned int size, unsigned int tuplesz, void* existingdata)
        {
            maxsize_ = size;
            tuplesize_ = tuplesz;
            data_ = existingdata;
            // invalidate free pointer, making page "full".
            free_ = reinterpret_cast<char*>(data_) + maxsize_;
        }

        virtual ~FakePage() { }
};




inline bool Buffer::is_valid_address(void *loc, unsigned int len)
{
    return (data_ <= loc) && (loc <= ((char*)data_ + maxsize_ - len));
}

inline bool Buffer::can_store(unsigned int len)
{
    return is_valid_address(free_,len);
}

inline void* Buffer::allocate(unsigned int len)
{
    if(!can_store(len))
        return 0; ///< return null ptr;
    void* ret = free_;
    free_ = reinterpret_cast<char*>(free_)+len;
    return ret;
}

inline void* Buffer::atomic_allocate(unsigned int len)
{
    void* oldval;
    void* newval;
    void** val = &free_;

    newval = *val;
    do
    {
        if (!can_store(len))
            return 0; //nullptr;

        oldval = newval;
        newval = (char*)oldval + len;
        newval = atomic_compare_and_swap(val, oldval, newval);
    }while(newval != oldval);
    return newval;
}

inline const unsigned int Buffer::get_used_space()
{
    return reinterpret_cast<char*>(free_) - reinterpret_cast<char*>(data_);
}

inline void* TupleBuffer::get_tuple_offset(unsigned int pos)
{
    char* f = reinterpret_cast<char*>(free_);
    char* d = reinterpret_cast<char*>(data_);
    char* ret = d + static_cast<unsigned long long>(pos)*tuplesize_;
    return ret < f ? ret : 0;
}

inline void* TupleBuffer::allocate_tuple()
{
    return Buffer::allocate(tuplesize_);
}

inline void* TupleBuffer::atomic_allocate_tuple()
{
    return Buffer::atomic_allocate(tuplesize_);
}

inline bool TupleBuffer::is_valid_tuple_address(void *loc)
{
    return Buffer::is_valid_address(loc,tuplesize_);
}











#endif // PAGE_H
