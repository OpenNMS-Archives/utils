#!/usr/bin/perl

use strict;
use warnings;

use IO::Handle;

my $file   = shift;
my $table  = lc(shift);
my $column = lc(shift);

usage() unless (defined $file and defined $table and defined $column);
usage() if ($file ne "-" and not -f $file);

my $in_copy = 0;
my $handle  = IO::Handle->new();
my $line;
my $colnum;

if ($file eq "-")
{
	$handle->fdopen(fileno(STDIN), "r") or die "unable to open STDIN for reading: $!\n";
}
else
{
	open ($handle, $file) or die "unable to open $file for reading: $!\n";
}

while ($line = <$handle>)
{
	if (not $in_copy and $line =~ /^\s*COPY (\S+) \(([^\)]+)\)/)
	{
		my $current_table = lc($1);
		my @columns       = split(/\s*,\s*/, $2);

		if ($current_table eq $table)
		{
			$in_copy = 1;
			for my $num (0..$#columns)
			{
				if (lc($columns[$num]) eq $column)
				{
					$colnum = $num;
					last;
				}
			}
		}
	}
	elsif ($in_copy and $line =~ /^\\\.$/)
	{
		last;
	}
	elsif (defined $colnum and $in_copy)
	{
		chomp($line);
		my @row = split(/\t/, $line);
		print $row[$colnum], "\n";
	}
}
close ($handle);

die "unable to find column '$column' in table '$table'" if (not defined $colnum);

sub usage
{
	print "usage: $0 </path/to/postgresql/dumpfile> <tablename> <columnname>\n\n";
	exit 1;
}

