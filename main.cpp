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
#include <cstdlib>
#include <ctime>
#include "common/rdtsc.h"

using namespace libconfig;
using namespace std;

#define random(x) (rand()%x)

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

unsigned long long timer1, timer2;

inline void initchkpt(void)
{
    startTimer(&timer1);
    startTimer(&timer2);
}

inline void buildchkpt(void)
{
    stopTimer(&timer1);
}

inline void probechkpt(void)
{
    stopTimer(&timer2);
}

inline void resetchkpt(void)
{
    timer1 = 0;
    timer2 = 0;
}

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
    unsigned int buffsize;
    unsigned int bucksize;
    vector<unsigned int> select1, select2;
    unsigned int num;
    unsigned int selectivity;
    unsigned long long total;
    unsigned long long cond_s, cond_e;

    Config cfg;

    cout << "Loading configuration file... " << flush;

    cfg.readFile(argv[1]);
    datapath = (const char*) cfg.lookup("path");
    outputfile = (const char*) cfg.lookup("output");

    bucksize = cfg.lookup("algorithm.buildpagesize");
    num  = cfg.lookup("algorithm.num");
    selectivity =  cfg.lookup("algorithm.selectivity");
    total = 1048576;/*(unsigned long long)cfg.lookup("algorithm.total");*/
    infilename = (const char*)cfg.lookup("build.file");
    buffsize = cfg.lookup("buffsize");
    sin = Schema::create(cfg.lookup("build.schema"));
    WriteTable wr1;
    wr1.init(&sin,buffsize);
    tin = &wr1;
    joinattr1 = cfg.lookup("build.jattr");
    joinattr1--;
    select1 = createIntVector(cfg.lookup("build.select"));

    sout = Schema::create(cfg.lookup("probe.schema"));
    WriteTable wr2;
    wr2.init(&sout,buffsize);
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
//    cout << "Running hash join algorithm!" << flush;

//    joiner->init(tin->schema(),select1,joinattr1,
//            tout->schema(),select2,joinattr2,selectivity);

    cout << endl;
    cout << "Finishing the joiner init!" << flush<<endl;

    ReuseCache *cache =(ReuseCache*)new ReuseCache(1024*1024);
    srand((int)time(0));
    ht_node* node = NULL;
    for(unsigned int i = 0; i < num; i++)
    {
        cout<<"Running hash join algorithm! Join No.: "<<i<<flush<<endl;
        cond_s = random(total/100*(100-selectivity));
        cond_e = cond_s + total/100*selectivity;
        cout<<"predication filter ["<<cond_s<<", "<<cond_e<<"]"<<flush<<endl;
        initchkpt();

        if(NULL == (node = cache->get_reusable_ht(cond_s,cond_e)))
        {
            node = cache->insert(cond_s,cond_e,bucksize,buffsize);
            joiner->init(tin->schema(),select1,joinattr1,
                         tout->schema(),select2,joinattr2,
                         selectivity,cond_s,cond_e);
            //node->hashtable_->print();
        }
        else
        {
            cout << "Got a reusable hashtable["<<node->start_value_<<","<<node->end_value_<<"]!" <<flush <<endl;
            //node->hashtable_->print();
            joiner->init(tin->schema(),select1,joinattr1,
                         tout->schema(),select2,joinattr2,
                         selectivity,cond_s,cond_e);
        }

        //cache.print_cache();
        joiner->build(tin,node);
        buildchkpt();
        //node->hashtable_->print();
        PageCursor* t = joiner->probe(tout,node);
        probechkpt();
        cout<< "RUNTIME TOTAL, BUILD_PART: "<<timer1<<" PROBE_PART: "<<timer2-timer1<<" TOTAL: "<<timer2<<endl;
        cout<<"Finshing hash join algorithm! Join No.: "<<i<<flush<<endl;
        t->close();
        delete t;
        cout<<endl;
        tin->reset();
        tout->reset();
        resetchkpt();
    }

//    joiner->build(tin);

//    PageCursor* t = joiner->probe(tout);

    joiner->destroy();
    cache->destroy();

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

    //t->close();

    //delete t;

    return 0;
}
