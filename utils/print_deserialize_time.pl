#!/usr/bin/perl

use strict;
use warnings;

while (<>) {
    if (/Deserialize Time\":(\d+)/) {
        print "time: $1\n";
    }
}
