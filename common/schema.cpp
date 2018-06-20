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

#include "schema.h"
#include<cassert>
#include<cstdlib>
#include<sstream>
#include<iomanip>
#include<algorithm>

using namespace std;


void Schema::add(ColumnSpec desc)
{
    add(desc.first,desc.second);
}

void Schema::add(ColumnType ct, unsigned int size)
{
    vct_.push_back(ct);
    offset_.push_back(totalsize_); ///<相比较之前的值，这个时候就是偏移量了

    int s = -1;
    switch (ct) {
        case CT_INTEGER:
        {
            s = sizeof(int);
            break;
        }
        case CT_LONG:
        {
            s = sizeof(long long);
            break;
        }
        case CT_DECIMAL:
        {
            s = sizeof(double);
            break;
        }
        case CT_CHAR:
        {
            s = size;
            break;
        }
        case CT_POINTER:
        {
            s = sizeof(void*);
            break;
        }
        default:
        {
            cout<<"illegal type!"<<endl;
            break;
        }
    }
    assert(s != -1);
    totalsize_ += s;
}

unsigned int Schema::columns()
{
    return vct_.size();
}

ColumnType Schema::get_column_type(unsigned int pos)
{
   return vct_[pos];
}

int Schema::get_column_type_size(unsigned int pos)
{
    int s = 0;
    switch (vct_[pos])
    {
        case CT_INTEGER:
        {
            s = sizeof(int);
            break;
        }
        case CT_LONG:
        {
            s = sizeof(long long);
            break;
        }
        case CT_DECIMAL:
        {
            s = sizeof(double);
            break;
        }
        case CT_CHAR:
        {
            s = 0;
            break;
        }
        case CT_POINTER:
        {
            s = sizeof(void*);
            break;
        }
        default:
        {
            cout<<"illegal type!"<<endl;
            break;
        }
    }
    return s;
}

ColumnSpec Schema::get(unsigned int pos)
{
    unsigned int val1 =  offset_[pos];
    unsigned int val2 = (pos != columns()-1) ? offset_[pos+1] : totalsize_;
    return make_pair(vct_[pos],val2-val1);
}


const char* Schema::as_string(void *data, unsigned int pos)
{
    char* d = reinterpret_cast<char*>(data);
    return reinterpret_cast<const char*>(d+offset_[pos]);
}

long long Schema::as_long(void *data, unsigned int pos)
{
    char* d = reinterpret_cast<char*>(data);
    return *reinterpret_cast<long long*>(d+offset_[pos]);
}


double Schema::as_double(void *data, unsigned int pos)
{
    char* d = reinterpret_cast<char*>(data);
    return *reinterpret_cast<double*>(d+offset_[pos]);
}

int Schema::as_int(void *data, unsigned int pos)
{
    char* d = reinterpret_cast<char*>(data);
    return *reinterpret_cast<int*>(d+offset_[pos]);
}

void* Schema::as_pointer(void *data, unsigned int pos)
{
    char* d = reinterpret_cast<char*> (data);
    return reinterpret_cast<void*>(*reinterpret_cast<long*> (d+offset_[pos]));
}


void* Schema::calc_offset(void *data, unsigned int pos)
{
    return reinterpret_cast<char*>(data)+offset_[pos];
}

void Schema::parse_tuple(void *dest, const std::vector<string> &input)
{
    const char** data = new const char*[columns()];
    for (unsigned int i=0; i<columns(); ++i) {
        data[i] = input[i].c_str();
    }
    parse_tuple(dest, data);
    delete[] data;
}

void Schema::parse_tuple(void *dest, const char **input)
{
    for(unsigned int i = 0; i < columns(); i++)
    {
        switch(vct_[i])
        {
            case CT_INTEGER:
                int val;
                val = atoi(input[i]);
                write_data(dest, i , &val);
                break;
            case CT_LONG:
                long long val3;
                val3 = atoll(input[i]);
                write_data(dest, i, &val3);
                break;
            case CT_DECIMAL:
                double val2;
                val2 = atof(input[i]);
                write_data(dest, i, &val2);
                break;
            case CT_CHAR:
                write_data(dest, i, input[i]);
                break;
            case CT_POINTER:
                cout<<"pointer cannot be transferred!"<<endl;
                break;
        }
    }
}

std::vector<std::string> Schema::output_tuple(void *data)
{
    vector<string> ret;
    for (unsigned int i=0; i<columns(); ++i)
    {
        ostringstream oss;
        switch (vct_[i])
        {
            case CT_INTEGER:
                oss << as_int(data, i);
                break;
            case CT_LONG:
                oss << as_long(data, i);
                break;
            case CT_DECIMAL:
                // every decimal in TPC-H has 2 digits of precision
                oss << setiosflags(ios::fixed) << setprecision(2) << as_double(data, i);
                break;
            case CT_CHAR:
                oss << as_string(data, i);
                break;
            case CT_POINTER:
                cout<<"pointer cannot be transferred!"<<endl;
                break;
        }
        ret.push_back(oss.str());
    }
    return ret;
}

Schema Schema::create(const libconfig::Setting &line)
{
    Schema ret;
    for(int i = 0; i < line.getLength(); ++i)
    {
        string val = line[i];
        transform(val.begin(),val.end(),val.begin(),::tolower);
        if (val.find("int")==0)
        {
            ret.add(CT_INTEGER);
        }
        else if (val.find("long")==0)
        {
            ret.add(CT_LONG);
        }
        else if (val.find("char")==0)
        {
            // char, check for integer
            string::size_type c = val.find("(");
            if (c==string::npos)
            {
                std::cout<<"npos!"<<std::endl;
                exit(0);
            }
            istringstream iss(val.substr(++c));
            int len;
            iss >> len;
            ret.add(CT_CHAR, len+1);	// compensating for \0
        }
        else if (val.find("dec")==0)
        {
            ret.add(CT_DECIMAL);
        }
        else
        {
            cout<<"ERROR! unknown type!"<<endl;
        }
    }
    return ret;
}

string Schema::pretty_print(void* tuple, char sep)
{
    string ret;
    const vector<string>& tokens = output_tuple(tuple);
    for (int i=0; i<tokens.size()-1; ++i)
        ret += tokens[i] + sep;
    if (tokens.size() > 1)
        ret += tokens[tokens.size()-1];
    return ret;
}

























