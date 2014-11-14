#!/usr/bin/perl

use strict;
use warnings;

#Executor Run Time":53,

while (<>) {
    if (/Executor Run Time\":(\d+)/) {
        print "time: $1\n";
    }
}
