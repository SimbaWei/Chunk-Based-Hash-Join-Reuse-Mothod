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

#ifndef SCHEMA_H
#define SCHEMA_H

#include<utility>
#include<vector>
#include<string>
#include<cstring>
#include<iostream>
#include "libconfig.h++"

using namespace std;

enum ColumnType {
    CT_INTEGER,  ///< Integer is sizeof(int), either int32_t or int64_t
    CT_LONG,     ///< Long is sizeof(long long), exactly 64 bits
    CT_DECIMAL,  ///< Decimal is sizeof(double)
    CT_CHAR,     ///< Char is variable length
    CT_POINTER,  ///< Pointer is sizeof(void*)
};

typedef std::pair<ColumnType, unsigned int> ColumnSpec;

class Schema
{
    public:
    Schema() : totalsize_(0) { }

    void add(ColumnType ct, unsigned int size = 0);

    void add(ColumnSpec desc);

    ColumnType get_column_type(unsigned int pos);

    int get_column_type_size(unsigned int pos);

    ColumnSpec get(unsigned int pos);

    unsigned int columns();

    unsigned int get_tuple_size();

    const char* as_string(void* data, unsigned int pos);

    void* calc_offset(void* data, unsigned int pos);

    int as_int(void* data, unsigned int pos);

    long long as_long(void* data, unsigned int pos);

    double as_double(void* data, unsigned int pos);

    void* as_pointer(void* data, unsigned int pos);

    void write_data(void* dest, unsigned int pos, const void* const data);

    void parse_tuple(void* dest, const std::vector<std::string>& input);

    void parse_tuple(void* dest, const char** input);

    std::vector<std::string> output_tuple(void* data);

    void copy_tuple(void* dest, const void* const src);

    static Schema create(const libconfig::Setting& line);

    string pretty_print(void* tuple, char sep);
    private:
        vector<ColumnType> vct_;
        vector<int> offset_; ///< be compared with the first column
        int totalsize_;
};


inline unsigned int Schema::get_tuple_size()
{
    return totalsize_;
}

inline void Schema::write_data(void* dest, unsigned int pos, const void * const data)
{
    void* d = reinterpret_cast<void*>(dest)+offset_[pos];

    switch(vct_[pos])
    {
        case CT_INTEGER:
        {
            const int* val = reinterpret_cast<const int*>(data);
            *reinterpret_cast<int*>(d) = *val;
            break;
        }
        case CT_LONG:
        {
            const long long* val = reinterpret_cast<const long long*>(data);
            *reinterpret_cast<long long*>(d) = *val;
            break;
        }
        case CT_DECIMAL:
        {
            const double* val = reinterpret_cast<const double*>(data);
            *reinterpret_cast<double*>(d) = *val;
            break;
        }
        case CT_POINTER:
        {
            const long* val = reinterpret_cast<const long*>(data);
            *reinterpret_cast<long*>(d) = *val;
            break;
        }
        case CT_CHAR:
        {
            const char* p = reinterpret_cast<const char*>(data);
            char* t = reinterpret_cast<char*>(d);

            //write p in t
            while (*(t++) = *(p++))
                ;
            break;
        }
        default:
        {
            cout<<"illegal type!"<<endl;
        }

    }
}

inline void Schema::copy_tuple(void *dest, const void * const src)
{
    memcpy(dest,src,totalsize_);
}





#endif // SCHEMA_H
