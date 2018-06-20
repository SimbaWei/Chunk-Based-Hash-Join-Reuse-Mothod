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

#include "page.h"
#include<cstring>
#include<iostream>

using namespace std;

Buffer::Buffer(void *data, unsigned int size, void *free)
    :data_(data),maxsize_(size),owner_(false),free_(free)
{
    if(NULL == free)
        this->free_ = reinterpret_cast<char*>(data)+size;
}

Buffer::Buffer(unsigned long size)
    :maxsize_(size),owner_(true)
{
    data_ = new char[size];
    free_ = data_;
}

Buffer::~Buffer()
{
    if(owner_)
        delete[] reinterpret_cast<char*>(data_);
    data_ = 0;
    free_ = 0;
    maxsize_ = 0;
    owner_ = false;
}

TupleBuffer::TupleBuffer(unsigned long size, unsigned int tuplesize)
    :Buffer(size), tuplesize_(tuplesize)
{
    if(size < tuplesize)
        cout<<"size must be greater than tuplesize! Failed to init!"<<endl;
}

TupleBuffer::TupleBuffer(void *data, unsigned int size, void *free, unsigned int tuplesize)
    :Buffer(data,size,free), tuplesize_(tuplesize)
{
    if(size < tuplesize)
        cout<<"size must be greater than tuplesize! Failed to init!"<<endl;
}

unsigned int page_copy(void* dest, const Buffer* page)
{
    unsigned int ret = ((char*)(page->free_)) - ((char*)(page->data_));
    memcpy(dest, page->data_, ret);
    return ret;
}











