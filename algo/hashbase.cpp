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

HashBase::HashBase(const libconfig::Setting &cfg)
    : BaseAlgo(cfg)
{
    size_ = cfg["algorithm"]["buildpagesize"];
    outputsize_ = cfg["bucksize"];
}

HashBase::~HashBase()
{

}

void HashBase::init(Schema *schema1, vector<unsigned int> select1, unsigned int jattr1,
  Schema *schema2, vector<unsigned int> select2, unsigned int jattr2, unsigned int selectivity, unsigned long long cond_s, unsigned long long cond_e)
{
    BaseAlgo::init(schema1,select1,jattr1,schema2,select2,jattr2,selectivity,cond_s,cond_e);
}

void HashBase::destroy()
{
    BaseAlgo::destroy();
}













