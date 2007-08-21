#!/usr/bin/perl

$|++;

use strict;
use warnings;

use Data::Dumper;
use DBI;
use Memoize;

my $current_table;
my $in_copy = 0;
my $columns = {};
my $node_mapping => {};
my $ifservices_mapping => {};

my $foreignfile = shift;
my $servicefile = shift;
my $inputfile   = shift;
my $dbname      = shift || 'opennms';
my $dbhost      = shift || 'localhost';

if (not defined $servicefile or not defined $inputfile or not -f $servicefile or not -f $inputfile or not defined $dbname) {
	print "usage: $0 <foreignid_file> <servicefile> <input.sql> [database] [database_host]\n\n";
	exit 1;
}

# this is a tab-separated file: nodeid,foreignid,foreignsource
open (FILEIN, $foreignfile) or die "can't read from $foreignfile: $!\n";
while (<FILEIN>)
{
	chomp;
	my ($nodeid, $foreignid, $foreignsource) = split(/\t/);
	$node_mapping->{$nodeid} = [ $foreignid, $foreignsource ];
}
close(FILEIN);

# this is a tab-separated file: nodeid,ipaddr,ifindex,serviceid
open (FILEIN, $servicefile) or die "can't read from $servicefile: $!\n";
while (<FILEIN>)
{
	chomp;
	my ($nodeid, $ipaddr, $ifindex, $serviceid) = split(/\t/);
	$ipaddr    = undef if ($ipaddr eq '\\N');
	$serviceid = undef if ($serviceid eq '\\N');
	$ifservices_mapping->{$ipaddr}->{$serviceid} = $nodeid;
}
close(FILEIN);

my $dbh = DBI->connect('dbi:Pg:host=' . $dbhost . ';dbname=' . $dbname, 'postgres');
my $select_nodeid = $dbh->prepare('SELECT nodeid FROM node WHERE foreignid=? AND foreignsource=?');
my $select_ifserviceid = $dbh->prepare('SELECT id FROM ifservices WHERE nodeid=? AND ipaddr=? AND serviceid=?');

sub get_new_nodeid($)
{
	my $nodeid = shift;

	#print "get_new_nodeid($nodeid)\n";

	my $new_nodeid;
	$select_nodeid->execute(@{$node_mapping->{$nodeid}});
	if ($select_nodeid->rows)
	{
		($new_nodeid) = $select_nodeid->fetchrow_array();
	}
	$select_nodeid->finish();
	return $new_nodeid;
}

sub get_new_ifserviceid($$$)
{
	my $nodeid    = shift;
	my $ipaddr    = shift;
	my $serviceid = shift;

	#print "get_new_ifserviceid($nodeid, $ipaddr, $serviceid)\n";

	my $new_ifserviceid;
	$select_ifserviceid->execute($nodeid, $ipaddr, $serviceid);
	if ($select_ifserviceid->rows)
	{
		($new_ifserviceid) = $select_ifserviceid->fetchrow_array();
	}
	$select_ifserviceid->finish();
	return $new_ifserviceid;
}

memoize('get_new_nodeid');
memoize('get_new_ifserviceid');

# get the events and outages, and translate them
if ($inputfile =~ /.gz$/)
{
	open (FILEIN, "gzip -dc $inputfile |") or die "can't read from sql file: $!\n";
}
else
{
	open (FILEIN, $inputfile) or die "can't read from sql file: $!\n";
}

open (FILEOUT, '>translated-outages.sql') or die "can't write to translated-outages.sql: $!\n";

while (my $line = <FILEIN>)
{
	if (not $in_copy and $line =~ /^\s*COPY\s+(\S+)\s+\(([^\)]+)\)/)
	{
		   $current_table = lc($1);
		my @columns       = split(/\s*,\s*/, $2);

		if ($current_table eq 'outages')
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
		next if (not defined $new_nodeid);
		next if ($nodeid eq $new_nodeid);

		if ($current_table eq 'outages')
		{
			my $ipaddr      = $row[$columns->{'ipaddr'}];
			my $serviceid   = $row[$columns->{'serviceid'}];
			my $ifserviceid = $row[$columns->{'ifserviceid'}];
	
			my $new_ifserviceid = get_new_ifserviceid($new_nodeid, $ipaddr, $serviceid);
			if (not defined $new_ifserviceid)
			{
				warn "could not determine new ifserviceid, skipping: $line\n";
			}
			else
			{
				print $current_table, ": nodeid(", $nodeid, " -> ", $new_nodeid, ")";
				if ($ifserviceid != $new_ifserviceid)
				{
					print ", ifserviceid(", $ifserviceid, " -> ", $new_ifserviceid, ")";
				}
				print "\n";
				$row[$columns->{'nodeid'}] = $new_nodeid;
				$row[$columns->{'ifserviceid'}] = $new_ifserviceid;
				print FILEOUT join("\t", @row), "\n";
			}
		}
	}
}
close (FILEOUT);
close (FILEIN);

$dbh->disconnect;
