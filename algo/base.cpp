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

#include "algo.h"
#include <utility>

using namespace std;

void BaseAlgo::init(Schema *schema1, vector<unsigned int> select1, unsigned int jattr1,
  Schema *schema2, vector<unsigned int> select2,
  unsigned int jattr2, unsigned int selectivity,
  unsigned long long cond_s, unsigned long long cond_e)
{
    s2_ = schema2;
    sel1_ = select1;
    sel2_ = select2;
    ja1_ = jattr1;
    ja2_ = jattr2;
    selectivity_ = selectivity;
    cond_s_ = cond_s;
    cond_e_ = cond_e;

    sout_ = new Schema();
    sbuild_ = new Schema();
    s1_ = new Schema();

    sbuild_->add(schema1->get(ja1_));
    for (vector<unsigned int>::iterator i1=sel1_.begin(); i1!=sel1_.end(); ++i1)
    {
        pair<ColumnType, unsigned int> ct = schema1->get(*i1);
        sout_->add(ct);
        sbuild_->add(ct);
        s1_->add(ct);
    }

    for (vector<unsigned int>::iterator i2=sel2_.begin(); i2!=sel2_.end(); ++i2)
    {
        sout_->add(s2_->get(*i2));
    }
}

void BaseAlgo::destroy()
{
    // XXX memory leak: can't delete, pointed to by output tables
    // delete sout;
    delete sbuild_;
    delete s1_;
    delete sout_;
}

BaseAlgo::BaseAlgo(const libconfig::Setting& cfg)
{

}

















