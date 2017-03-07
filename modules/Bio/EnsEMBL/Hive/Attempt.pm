=pod

=head1 NAME

    Bio::EnsEMBL::Hive::Attempt

=head1 DESCRIPTION

    An object to describe an attempt of a job.
    It is stored in its own table (attempt) indexed by a dbID, and is thus Storable

=head1 LICENSE

    Copyright [1999-2015] Wellcome Trust Sanger Institute and the EMBL-European Bioinformatics Institute
    Copyright [2016-2017] EMBL-European Bioinformatics Institute

    Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software distributed under the License
    is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and limitations under the License.

=head1 CONTACT

    Please subscribe to the Hive mailing list:  http://listserver.ebi.ac.uk/mailman/listinfo/ehive-users  to discuss Hive-related questions or to be notified of our updates

=cut


package Bio::EnsEMBL::Hive::Attempt;

use strict;
use warnings;

use base ( 'Bio::EnsEMBL::Hive::Storable' );


# ----------------------------- Cacheable implementation -----------------------------------

sub unikey {
    return [ 'job_id', 'retry_index' ];
}


# --------------------------------- Getter / Setters ---------------------------------------

sub status {
    my $self = shift;
    $self->{'_status'} = shift if(@_);
    return $self->{'_status'} || 'INITIALIZATION';
}


sub when_initialized {
    my $self = shift;
    $self->{'_when_initialized'} = shift if(@_);
    return $self->{'_when_initialized'};
}


sub when_updated {
    my $self = shift;
    $self->{'_when_updated'} = shift if(@_);
    return $self->{'_when_updated'};
}

sub runtime_msec {
    my $self = shift;
    $self->{'_runtime_msec'} = shift if(@_);
    return $self->{'_runtime_msec'};
}

sub query_count {
    my $self = shift;
    $self->{'_query_count'} = shift if(@_);
    return $self->{'_query_count'};
}

sub retry_index {
    my $self = shift;
    $self->{'_retry_index'} = shift if(@_);
    $self->{'_retry_index'} = 0 unless(defined($self->{'_retry_index'}));
    return $self->{'_retry_index'};
}

sub stdout_file {
  my $self = shift;
  $self->{'_stdout_file'} = shift if(@_);
  return $self->{'_stdout_file'};
}

sub stderr_file {
  my $self = shift;
  $self->{'_stderr_file'} = shift if(@_);
  return $self->{'_stderr_file'};
}


##-----------------[indicators to the Worker]--------------------------------

# Job can set this to "role", "worker" or "beekeeper" prior to dying (or before running code that might cause death - such as RunnableDB's compilation)
# if it believes that the state of things will not allow the agentr to continue normally.
# The agent will check the value and commit suicide if it is set to true.
sub lethality_level {
    my $self = shift;
    $self->{'_lethality_level'} = shift if(@_);
    return lc $self->{'_lethality_level'} if defined $self->{'_lethality_level'};
}


sub failure_level {       # Job should set this to 1 prior to dying (or before running code that might cause death)
                            # if it believes that it makes sense to retry the same job without any changes.
                            # It may also set it to 0 prior to dying (or before running code that might cause death)
                            # if it believes that there is no point in re-trying (say, if the parameters are wrong).
                            # The Worker will check the flag and make necessary adjustments to the database state.
    my $self = shift;
    $self->{'_failure_level'} = shift if(@_);
    return lc($self->{'_failure_level'} // 'attempt');
}


sub incomplete {            # Job should set this to 0 prior to throwing if the job is done,
                            # but it wants the thrown message to be recorded with is_error=0.
    my $self = shift;
    $self->{'_incomplete'} = shift if(@_);
    return $self->{'_incomplete'};
}


sub died_somewhere {
    my $self = shift;

    $self->{'_died_somewhere'} ||= shift if(@_);    # NB: the '||=' only applies in this case - do not copy around!
    return $self->{'_died_somewhere'} ||=0;
}

##-----------------[/indicators to the Worker]-------------------------------


# --------------------------------- Compound methods ---------------------------------------

sub toString {
    my $self = shift @_;

    # $self->retry_index starts at 0 !
    my $suffix = 'th';
       $suffix = 'st' if ($self->retry_index % 10 == 0) && ($self->retry_index != 10);
       $suffix = 'nd' if ($self->retry_index % 10 == 1) && ($self->retry_index != 11);
       $suffix = 'rd' if ($self->retry_index % 10 == 2) && ($self->retry_index != 12);
    return ($self->retry_index+1).$suffix.' attempt of '.$self->job->toString;
}


# -------------------------------- Convenient methods --------------------------------------

=head2 set_and_update_status

  Example     : $attempt->set_and_update_status('WRITE_OUTPUT');
  Description : Sets the status of the attempt (within the job's life-cycle) and updates
                the database accordingly.
  Returntype  : none
  Exceptions  : none
  Caller      : general
  Status      : Stable

=cut

sub set_and_update_status {
    my ($self, $status) = @_;

    $self->status($status);

    if(my $adaptor = $self->adaptor) {
        $adaptor->check_in_attempt($self);
    }
}


1;

