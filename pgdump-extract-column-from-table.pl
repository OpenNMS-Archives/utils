#!/usr/bin/perl

use strict;
use warnings;

use Getopt::Long;
use IO::Handle;

my $ignore_if_null;

Getopt::Long::Configure(qw(bundling no_getopt_compat));
GetOptions( 'ignore-if-null=s' => \$ignore_if_null );

my $file   = shift;
my $table  = lc(shift);
my $column = lc(shift);

usage() unless (defined $file and defined $table and defined $column);
usage() if ($file ne "-" and not -f $file);

my $in_copy = 0;
my $handle  = IO::Handle->new();
my $line;
my $colnum;
my $ignore_colnum;

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
				}
				elsif (lc($columns[$num]) eq lc($ignore_if_null))
				{
					$ignore_colnum = $num;
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
		print $row[$colnum], "\n" unless (defined $ignore_colnum and $row[$ignore_colnum] eq '\\N');
	}
}
close ($handle);

die "unable to find column '$column' in table '$table'" if (not defined $colnum);

sub usage
{
	print <<END;
usage: $0 [options] </path/to/postgresql/dumpfile> <tablename> <columnname>

	--ignore-if-null=<colname>   ignore the row if the specified column is null

END
	exit 1;
}

