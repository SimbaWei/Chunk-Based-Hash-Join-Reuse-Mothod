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

#ifndef LOADER_H
#define LOADER_H

#include "table.h"

#include <string>
using std::string;


class Loader
{
    public:
        Loader(const char separator);

        void load(const string& filename, WriteTable& output);

    private:
        bool is_bz2(const string& filename);

        char* read_full_line(char* cur, const char* buf_start, const int buf_len);

        const char sep_;

        static const unsigned int MAX_LINE_ = 1024;

        static const unsigned int MAX_COL_ = 64;
};

#endif // LOADER_H
