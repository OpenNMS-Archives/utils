#!/usr/bin/perl

use strict;
use warnings;

use Getopt::Long;
use IO::Handle;

my $ignore_if_null;
my $print_columns;

Getopt::Long::Configure(qw(bundling no_getopt_compat));
GetOptions( 'ignore-if-null=s' => \$ignore_if_null, 'print-columns=s' => \$print_columns );

my $file   = shift;
my $table  = lc(shift);
my $column = lc(shift);

usage() unless (defined $file and defined $table and defined $column);
usage() if ($file ne "-" and not -f $file);

my $in_copy = 0;
my $handle  = IO::Handle->new();
my $line;
my $colnum;
my $columns = {};
my $ignore_colnum;
my @print_columns = split(/\s*,\s*/, $print_columns || $column);

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
				my $name = lc($columns[$num]);
				$columns->{$name} = $num;
				if ($name eq $column)
				{
					$colnum = $num;
				}
				elsif ($name eq lc($ignore_if_null))
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

		if (not defined $ignore_colnum or $row[$ignore_colnum] ne '\\N')
		{
			print join("\t", map { $row[$columns->{$_}] } @print_columns), "\n";
		}
		#print $row[$colnum], "\n" unless (defined $ignore_colnum and $row[$ignore_colnum] eq '\\N');
	}
}
close ($handle);

die "unable to find column '$column' in table '$table'" if (not defined $colnum);

sub usage
{
	print <<END;
usage: $0 [options] </path/to/postgresql/dumpfile> <tablename> <columnname>

	--ignore-if-null=<colname>   ignore the row if the specified column is null
	--print-columns=<colnames>   print the comma-separated list of columns
	                             when displaying matching rows (default: the
	                             column being filtered on)

END
	exit 1;
}

