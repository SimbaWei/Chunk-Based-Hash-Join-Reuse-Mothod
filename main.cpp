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


#include <iostream>
#include <fstream>
#include "common/schema.h"
#include "common/table.h"
#include "assert.h"
#include <libconfig.h++>
#include "algo/algo.h"
#include "joinerfactory.h"

using namespace libconfig;
using namespace std;

vector<unsigned int> createIntVector(const Setting& line)
{
    vector<unsigned int> ret;
    for (int i=0; i<line.getLength(); ++i)
    {
        unsigned int val = line[i];
        ret.push_back(val-1);
    }
    return ret;
}


BaseAlgo* joiner;
unsigned int joinattr1, joinattr2;

int main(int argc, char** argv)
{
    // must specify one file
    if(argc <= 1)
    {
        cout<< "There is no specified file!"<<endl;
    }
    assert(sizeof(char) == 1);
    assert(sizeof(void*) == sizeof(long));

    Table* tin, * tout;
    Schema sin, sout;
    string datapath, infilename, outfilename, outputfile;
    unsigned int bucksize;
    vector<unsigned int> select1, select2;

    Config cfg;

    cout << "Loading configuration file... " << flush;

    cfg.readFile(argv[1]);
    datapath = (const char*) cfg.lookup("path");
    outputfile = (const char*) cfg.lookup("output");

    infilename = (const char*)cfg.lookup("build.file");
    bucksize = cfg.lookup("bucksize");
    sin = Schema::create(cfg.lookup("build.schema"));
    WriteTable wr1;
    wr1.init(&sin,bucksize);
    tin = &wr1;
    joinattr1 = cfg.lookup("build.jattr");
    joinattr1--;
    select1 = createIntVector(cfg.lookup("build.select"));

    sout = Schema::create(cfg.lookup("probe.schema"));
    WriteTable wr2;
    wr2.init(&sout,bucksize);
    tout = &wr2;
    outfilename = (const char*) cfg.lookup("probe.file");
    joinattr2 = cfg.lookup("probe.jattr");
    joinattr2--;
    select2 = createIntVector(cfg.lookup("probe.select"));

    assert(tin->schema()->get_column_type(joinattr1) == CT_LONG);
    assert(tout->schema()->get_column_type(joinattr2) == CT_LONG);

    joiner = JoinerFactory::createJoiner(cfg);

    cout << "OK" << endl;




    // load files in memory
    cout << "Loading files in memory..." << flush;

    wr1.load(datapath+infilename, "|");

    wr2.load(datapath+outfilename, "|");

    //wr1.print_table();

    //wr2.print_table();

    cout << "OK" << endl;

    // run hash join
    cout << "Running hash join algorithm!" << flush;

    joiner->init(tin->schema(),select1,joinattr1,
            tout->schema(),select2,joinattr2);

    cout << endl;
    cout << "Finishing the joiner init!" << flush;

    joiner->build(tin);

    PageCursor* t = joiner->probe(tout);

    joiner->destroy();

    cout << "OK" << endl;

#ifdef DEBUG
    cout << "Outputting code for validation... " << flush;
    ofstream foutput((datapath+outputfile).c_str());
    if(t)
    {
        void* tuple;
        while (b = t->read_next()) {
            int i = 0;
            while(tuple = b->getTupleOffset(i++)) {
                foutput << t->schema()->pretty_print(tuple, '|') << '\n';
            }
        }
    }
    foutput << flush;
    foutput.close();
    cout << "ok" << endl;
#endif

    wr1.close();

    wr2.close();

    t->close();

    delete t;

    return 0;
}
