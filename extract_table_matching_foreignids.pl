#!/usr/bin/perl

$|++;

use strict;
use warnings;

use Data::Dumper;
use DBI;
use Memoize;

memoize('get_new_nodeid');

my $current_table;
my $in_copy = 0;
my $columns = {};
my $nodes = {};
my $nodes_to_new_nodes = {};

my $idfile    = shift;
my $inputfile = shift;

if (not defined $idfile or not defined $inputfile or not -f $idfile or not -f $inputfile) {
	print "usage: $0 <idfile> <input.sql>\n\n";
	exit 1;
}

# this is a tab-separated file: <nodeid>	<foreignid>	<foreignsource>
open (FILEIN, $idfile) or die "can't read from foreignids.txt: $!\n";
while (<FILEIN>)
{
	chomp;
	my @row = split(/\t/);
	$nodes->{$row[0]} = [ $row[1], $row[2] ];
}
close(FILEIN);

my $dbh = DBI->connect('dbi:Pg:dbname=test', 'postgres');
my $select_nodeid = $dbh->prepare('SELECT nodeid FROM node WHERE foreignid=? AND foreignsource=?');

# get the events and outages, and translate them
open (FILEIN, $inputfile) or die "can't read from sql file: $!\n";
open (FILEOUT, '>translated-events.sql') or die "can't write to translated-events.sql: $!\n";

while (my $line = <FILEIN>)
{
	if (not $in_copy and $line =~ /^\s*COPY\s+(\S+)\s+\(([^\)]+)\)/)
	{
		   $current_table = lc($1);
		my @columns       = split(/\s*,\s*/, $2);

		if ($current_table eq 'events')
		{
			$in_copy = 1;
			$columns = {};
			for my $num (0..$#columns)
			{
				my $name = lc($columns[$num]);
				$columns->{$name} = $num;
			}
			print FILEOUT $line;
		}
	}
	elsif ($in_copy and $line =~ /^\\\.$/)
	{
		$in_copy = 0;
		print FILEOUT $line;
	}
	elsif ($in_copy)
	{
		chomp($line);
		my @row = split(/\t/, $line);

		my $nodeid = $row[$columns->{'nodeid'}];
		next if ($nodeid eq '\\N');
		my $new_nodeid = get_new_nodeid($nodeid);
		next unless (defined $new_nodeid);
		next if ($nodeid eq $new_nodeid);

		print $current_table, ": ", $nodeid, " -> ", $new_nodeid, "\n";
		$row[$columns->{'nodeid'}] = $new_nodeid;
		print FILEOUT join("\t", @row), "\n";
	}
}
close (FILEOUT);
close (FILEIN);

$dbh->disconnect;

sub get_new_nodeid
{
	my $nodeid = shift;
	my $new_nodeid;
	$select_nodeid->execute(@{$nodes->{$nodeid}});
	if ($select_nodeid->rows)
	{
		($new_nodeid) = $select_nodeid->fetchrow_array();
	}
	$select_nodeid->finish();
	$nodes_to_new_nodes->{$nodeid} = $new_nodeid;
	return $new_nodeid;
}
