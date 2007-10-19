#!/usr/bin/perl

use strict;
use warnings;

use XML::Simple;

my $snmpgraph_properties = shift;
my $fromfile             = shift;
my $tofile               = shift;

my $report_types = {};

if (not defined $tofile) {
	print "usage: $0 <snmp-graph.properties> <input_file> [output_file]\n";
	exit 1;
}

open (FILEIN, $snmpgraph_properties) or die "unable to open $snmpgraph_properties for reading: $!\n";
while (my $line = <FILEIN>) {
	chomp($line);
	if (my ($report_name, $type) = $line =~ /^\s*report\.(.*)\.type\s*=\s*(.*?)\s*$/) {
		$report_types->{$report_name} = $type;
	}
}
close (FILEIN);

my $ref = XMLin($fromfile, ForceArray => 1);

$ref->{'xmlns'} = 'http://xmlns.opennms.org/xsd/config/kscReports';

if (exists $ref->{'Report'} and defined $ref->{'Report'}) {
	for my $report (@{$ref->{'Report'}}) {
		$report->{'graphs_per_line'} = 0;
	
		if (exists $report->{'Graph'} and defined $report->{'Graph'}) {
			for my $graph (@{$report->{'Graph'}}) {
				if (exists $graph->{'nodeId'} and exists $graph->{'interfaceId'} and exists $graph->{'graphtype'}) {
					my $nodeid      = $graph->{'nodeId'};
					my $interfaceid = $graph->{'interfaceId'};
					my $graphtype   = $graph->{'graphtype'};
			
					my $type = $report_types->{$graphtype};
			
					if ($type eq "nodeSnmp") {
						$graph->{'resourceId'} = "node[$nodeid].nodeSnmp[]";
					} else {
						$graph->{'resourceId'} = "node[$nodeid].interfaceSnmp[$interfaceid]";
					}
					delete $graph->{'nodeId'};
					delete $graph->{'interfaceId'};
				}
			}
		}
	}
}

my $output = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" . XMLout($ref, RootName => 'ReportsList');

if (defined $tofile) {
	open (FILEOUT, '>' . $tofile) or die "unable to write to '$tofile': $!";
	print FILEOUT $output;
	close (FILEOUT);
} else {
	print $output;
}
