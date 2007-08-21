#!/usr/bin/perl

use strict;
use warnings;
use DBI;

my $dbh = DBI->connect('dbi:Pg:dbname=test', 'postgres', '', { AutoCommit => 0 }) or die "can't connect: " . DBI->errstr;
my $select_nodeid = $dbh->prepare("SELECT nodeid FROM node WHERE foreignid=? AND foreignsource=?");

open (FILEIN, "foreignids.txt") or die "can't read from foreignids.txt: $!\n";
while (<FILEIN>) {
	chomp;
	my ($nodeid, $foreignid, $foreignsource) = split(/\t/);
	$select_nodeid->execute($foreignid, $foreignsource) or die "can't select for ($foreignid, $foreignsource): " . $select_nodeid->errstr;
	if ($select_nodeid->rows()) {
		my ($new_nodeid) = $select_nodeid->fetchrow_array();
		if ($nodeid != $new_nodeid) {
			print "$nodeid\t$new_nodeid\n";
		}
	}
	$select_nodeid->finish();
}
close (FILEIN);

$dbh->disconnect();
