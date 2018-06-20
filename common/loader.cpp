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

#include <fstream>
using std::ifstream;

#include "bzlib.h"

#include "loader.h"
#include "parser.h"
#include <iostream>
#include "assert.h"
#include "exceptions.h"

Loader::Loader(const char separator)
    : sep_(separator)
{

}

/**
 * Loads \a filename, parses it and outputs it at the WriteTable \a output.
 * Automatically checks if input is a BZ2 file and uncompresses on the fly,
 * using libbz2.
 */
void Loader::load(const string &filename, WriteTable &output)
{
    const char* parse_result[MAX_COL_];
    int parse_result_count;

    Parser parser(sep_);

    if(!is_bz2(filename))
    {
        char line_buf[MAX_LINE_];
        ifstream f(filename.c_str(), ifstream::in);

        if(!f)
        {
            std::cout<<"ERROR File Open!"<<std::endl;
            throw NotYetImplemented();
        }
        f.getline(line_buf,MAX_LINE_);
        while(f)
        {
            parse_result_count = parser.parse_line(line_buf,parse_result);
            output.append(parse_result,parse_result_count);
            f.getline(line_buf,MAX_LINE_);
        }
        f.close();
    }
    else
    {
        FILE* f;
        BZFILE* b;
        int bzerror;
        int nbuf;
        int nwritten;

        int unused = 0;

        char* decbuf;
        const int decbufsize = 1024*1024;
        decbuf = new char[decbufsize];

        f = fopen(filename.c_str(),"rb");
        if(!f)
        {
            std::cout<<"ERROR File Open!"<<std::endl;
            throw NotYetImplemented();
        }
        b = BZ2_bzReadOpen(&bzerror,f,0,0,NULL,0);
        if(bzerror != BZ_OK)
        {
            BZ2_bzReadClose (&bzerror, b);
            std::cout<<"Failed to open!"<<std::endl;
            throw NotYetImplemented();
        }
        bzerror = BZ_OK;

        while(bzerror == BZ_OK)
        {
            nbuf = BZ2_bzRead ( &bzerror, b, decbuf+unused, decbufsize-unused);
            if (bzerror != BZ_OK && bzerror != BZ_STREAM_END)
            {
                BZ2_bzReadClose ( &bzerror, b );
                throw NotYetImplemented();
            }
            char* p = decbuf;
            char* usablep = decbuf;
            assert(nbuf + unused <= decbufsize);
            while (p < decbuf + nbuf + unused)
            {
                p = read_full_line(usablep, decbuf, nbuf + unused);

                // Check if buffer didn't have a full line. If yes, break.
                //
                if (usablep == p)
                {
                    break;
                }

                // We have a full line at usablep. Parse it.
                //
                parse_result_count = parser.parse_line(usablep, parse_result);
                output.append(parse_result, parse_result_count);

                usablep = p;
            }
            // Copy leftovers to start of buffer and call bzRead so
            // that it writes output immediately after existing data.
            unused = decbuf + nbuf + unused - p;
            assert(unused < p - decbuf);  // no overlapped memcpy
            memcpy(decbuf, p, unused);
        }
        assert (bzerror == BZ_STREAM_END);
        BZ2_bzReadClose ( &bzerror, b );

        delete[] decbuf;
    }
}


/**
 * Reads one full line from buffer.
 *
 * If buffer has at least one line, returns the start of the next line and
 * \a cur points to a null-terminated string.
 *
 * Otherwise, returns \a cur.
 */
char* Loader::read_full_line(char* cur, const char* buf_start, const int buf_len)
{
    char* oldcur = cur;

    while(cur >= buf_start && cur < (buf_start + buf_len))
    {
        if ((*cur) == '\n')
        {
            *cur = 0;
            return ++cur;
        }

        cur++;
    }

    return oldcur;
}


/**
 * Returns true if input file is BZ2-compressed.
 * Check is done by looking at the three first bytes of the file: if "BZh",
 * then it is assumed to be a valid file to be parsed by libbz2.
 */
bool Loader::is_bz2(const string& filename)
{
    char header[4];

    ifstream f(filename.c_str(), ifstream::in);
    f.get(header, 4);
    f.close();

    return string(header) == "BZh";
}









