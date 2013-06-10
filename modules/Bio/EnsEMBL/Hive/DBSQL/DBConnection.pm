
=pod 

=head1 NAME

    Bio::EnsEMBL::Hive::DBSQL::DBConnection

=head1 SYNOPSIS

    my $url = $dbc->url();

=head1 DESCRIPTION

    Extends the functionality of Bio::EnsEMBL::DBSQL::DBConnection with things needed by the Hive

=head1 CONTACT

    Please contact ehive-users@ebi.ac.uk mailing list with questions/suggestions.

=cut

package Bio::EnsEMBL::Hive::DBSQL::DBConnection;

use strict;
use warnings;

use base ('Bio::EnsEMBL::DBSQL::DBConnection');


=head2 url

    Arg [1]    : String $environment_variable_name_to_store_password_in (optional)
    Example    : $url = $dbc->url;
    Description: Constructs a URL string for this database connection.
    Returntype : string of format  mysql://<user>:<pass>@<host>:<port>/<dbname>
                               or  sqlite:///<dbname>
    Exceptions : none
    Caller     : general

=cut

sub url {
    my ($self, $psw_env_var_name) = @_;

    my $url = $self->driver . '://';

    if($self->username) {
        $url .= $self->username;

        if(my $psw_expression = $self->password) {
            if($psw_env_var_name) {
                $ENV{$psw_env_var_name} = $psw_expression;
                $psw_expression = '${'.$psw_env_var_name.'}';
            }
            $url .= ':'.$psw_expression if($psw_expression);
        }

        $url .= '@';
    }

    if($self->host) {
        $url .= $self->host;

        if($self->port) {
            $url .= ':'.$self->port;
        }
    }
    $url .= '/' . $self->dbname;

    return $url;
}


sub protected_prepare_execute {     # try to resolve certain mysql "Deadlocks" by trying again (a useful workaround even in mysql 5.1.61)
    my $self        = shift @_;
    my $sql         = shift @_;

    my $retries     = 3;

    foreach (0..$retries) {
        eval {
            my $sth = $self->prepare($sql);
            $sth->execute( @_ );
            $sth->finish;
            1;
        } or do {
            if($@ =~ /Deadlock found when trying to get lock; try restarting transaction/) {    # ignore this particular error
                sleep 1;
                next;
            }
            die $@;     # but definitely report other errors
        };
        last;
    }
    die "After $retries retries still in a deadlock: $@" if($@);
}

1;
