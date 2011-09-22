#!/bin/csh -f
# Minimal compile script for mom4p1 and ice coupled experiments

set echo
set platform      = ia64           # A unique identifier for your platform
                                   # It could be one of: ia64, ibm, ifc, nec, pgi, sgi, t3e, t90 
                                   # This corresponds to the mkmf templates in $root/bin dir.
set MPI           = 1              # 1 if you want to use MPI, 0 otherwise
#
# User does not need to change anything below!
#
set type          = mom4p1_coupled                    # Name of the experiment
set root          = $cwd:h                            # The directory you created when you checkout
set code_dir      = $root/src                         # source code directory
set executable    = $root/exec_$platform/$type/fms_$type.x      # executable created after compilation
set pathnames     = $code_dir/path_names_$type        # path to file containing list of source paths
set mppnccombine  = $root/bin/mppnccombine.$platform  # path to executable mppnccombine
set mkmfTemplate  = $root/bin/mkmf.template.$platform # path to template for your platform
set mkmf          = $root/bin/mkmf                    # path to executable mkmf
set cppDefs       = ( "-Duse_netCDF -Duse_netCDF3 -DUSE_OCEAN_BGC -DENABLE_ODA" ) # list of cpp #defines to be passed to the source files
if($MPI) set cppDefs  = ( "-Duse_netCDF -Duse_netCDF3 -Duse_libMPI -DUSE_OCEAN_BGC -DENABLE_ODA" )

set static        = 0              # 1 if you want static memory allocation, 0 for dynamic
if($static) then
  set executable = $root/exec_$platform/${type}_static/fms_$type.x 
  set cppDefs = "$cppDefs -DMOM4_STATIC_ARRAYS -DNI_=360 -DNJ_=200 -DNK_=50 -DNI_LOCAL_=60 -DNJ_LOCAL_=50"
endif

#
# Users must ensure the correct environment file exists for their platform.
#
source $root/bin/environs.$platform  # environment variables and loadable modules

# setup directory structure
  if ( ! -d $executable:h )    mkdir -p $executable:h

#
# compile mppnccombine.c, needed only if $npes > 1
#NOTE: On some platforms you may need to specify the location for netcdf.h and libnetcdf.a
#      by modifying the following -I and -L
  if ( $MPI && ! -f $mppnccombine ) then
    cc -O -o $mppnccombine -I/usr/local/include -L/usr/local/lib $code_dir/postprocessing/mppnccombine.c -lnetcdf
  endif



# The list of source files that should be compiled for this experiment.
cat > $pathnames <<EOF
atmos_param/diag_integral/diag_integral.F90
atmos_param/monin_obukhov/monin_obukhov.F90
atmos_param/monin_obukhov/monin_obukhov_interfaces.h
atmos_param/monin_obukhov/monin_obukhov_kernel.F90
shared/amip_interp/amip_interp.F90
shared/astronomy/astronomy.F90
shared/axis_utils/axis_utils.F90
shared/column_diagnostics/column_diagnostics.F90
shared/constants/constants.F90
shared/coupler/atmos_ocean_fluxes.F90
shared/coupler/coupler_types.F90
shared/coupler/ensemble_manager.F90
shared/data_override/data_override.F90
shared/diag_manager/diag_axis.F90
shared/diag_manager/diag_data.F90
shared/diag_manager/diag_grid.F90
shared/diag_manager/diag_manager.F90
shared/diag_manager/diag_output.F90
shared/diag_manager/diag_util.F90
shared/drifters/cloud_interpolator.F90
shared/drifters/drifters.F90
shared/drifters/drifters_comm.F90
shared/drifters/drifters_compute_k.h
shared/drifters/drifters_core.F90
shared/drifters/drifters_input.F90
shared/drifters/drifters_io.F90
shared/drifters/drifters_push.h
shared/drifters/drifters_set_field.h
shared/drifters/fms_switches.h
shared/drifters/quicksort.F90
shared/exchange/stock_constants.F90
shared/exchange/xgrid.F90
shared/fft/fft.F90
shared/fft/fft99.F90
shared/field_manager/field_manager.F90
shared/field_manager/fm_util.F90
shared/field_manager/parse.inc
shared/fms/fms.F90
shared/fms/fms_io.F90
shared/fms/read_data_2d.inc
shared/fms/read_data_3d.inc
shared/fms/read_data_4d.inc
shared/fms/test_fms_io.F90
shared/fms/write_data.inc
shared/horiz_interp/horiz_interp.F90
shared/horiz_interp/horiz_interp_bicubic.F90
shared/horiz_interp/horiz_interp_bilinear.F90
shared/horiz_interp/horiz_interp_conserve.F90
shared/horiz_interp/horiz_interp_spherical.F90
shared/horiz_interp/horiz_interp_type.F90
shared/include/fms_platform.h
shared/memutils/memuse.c
shared/memutils/memutils.F90
shared/mosaic/constant.h
shared/mosaic/create_xgrid.c
shared/mosaic/create_xgrid.h
shared/mosaic/gradient.F90
shared/mosaic/gradient_c2l.c
shared/mosaic/gradient_c2l.h
shared/mosaic/grid.F90
shared/mosaic/interp.c
shared/mosaic/interp.h
shared/mosaic/mosaic.F90
shared/mosaic/mosaic_util.c
shared/mosaic/mosaic_util.h
shared/mosaic/read_mosaic.c
shared/mosaic/read_mosaic.h
shared/mpp/mpp.F90
shared/mpp/mpp_data.F90
shared/mpp/mpp_domains.F90
shared/mpp/mpp_io.F90
shared/mpp/mpp_memutils.F90
shared/mpp/mpp_parameter.F90
shared/mpp/mpp_pset.F90
shared/mpp/mpp_utilities.F90
shared/mpp/nsclock.c
shared/mpp/test_mpp.F90
shared/mpp/test_mpp_domains.F90
shared/mpp/test_mpp_io.F90
shared/mpp/test_mpp_pset.F90
shared/mpp/threadloc.c
shared/mpp/include/mpp_chksum.h
shared/mpp/include/mpp_chksum_int.h
shared/mpp/include/mpp_chksum_scalar.h
shared/mpp/include/mpp_comm.inc
shared/mpp/include/mpp_comm_mpi.inc
shared/mpp/include/mpp_comm_nocomm.inc
shared/mpp/include/mpp_comm_sma.inc
shared/mpp/include/mpp_data_mpi.inc
shared/mpp/include/mpp_data_nocomm.inc
shared/mpp/include/mpp_data_sma.inc
shared/mpp/include/mpp_do_check.h
shared/mpp/include/mpp_do_checkV.h
shared/mpp/include/mpp_do_get_boundary.h
shared/mpp/include/mpp_do_global_field.h
shared/mpp/include/mpp_do_redistribute.h
shared/mpp/include/mpp_do_update.h
shared/mpp/include/mpp_do_updateV.h
shared/mpp/include/mpp_do_updateV_ad.h
shared/mpp/include/mpp_do_update_ad.h
shared/mpp/include/mpp_domains_comm.inc
shared/mpp/include/mpp_domains_define.inc
shared/mpp/include/mpp_domains_misc.inc
shared/mpp/include/mpp_domains_reduce.inc
shared/mpp/include/mpp_domains_util.inc
shared/mpp/include/mpp_error_a_a.h
shared/mpp/include/mpp_error_a_s.h
shared/mpp/include/mpp_error_s_a.h
shared/mpp/include/mpp_error_s_s.h
shared/mpp/include/mpp_get_boundary.h
shared/mpp/include/mpp_global_field.h
shared/mpp/include/mpp_global_reduce.h
shared/mpp/include/mpp_global_sum.h
shared/mpp/include/mpp_global_sum_ad.h
shared/mpp/include/mpp_global_sum_tl.h
shared/mpp/include/mpp_io_connect.inc
shared/mpp/include/mpp_io_misc.inc
shared/mpp/include/mpp_io_read.inc
shared/mpp/include/mpp_io_util.inc
shared/mpp/include/mpp_io_write.inc
shared/mpp/include/mpp_read_2Ddecomp.h
shared/mpp/include/mpp_reduce_mpi.h
shared/mpp/include/mpp_reduce_nocomm.h
shared/mpp/include/mpp_reduce_sma.h
shared/mpp/include/mpp_sum.inc
shared/mpp/include/mpp_sum_mpi.h
shared/mpp/include/mpp_sum_nocomm.h
shared/mpp/include/mpp_sum_sma.h
shared/mpp/include/mpp_transmit.inc
shared/mpp/include/mpp_transmit_mpi.h
shared/mpp/include/mpp_transmit_nocomm.h
shared/mpp/include/mpp_transmit_sma.h
shared/mpp/include/mpp_update_domains2D.h
shared/mpp/include/mpp_update_domains2D_ad.h
shared/mpp/include/mpp_util.inc
shared/mpp/include/mpp_util_mpi.inc
shared/mpp/include/mpp_util_nocomm.inc
shared/mpp/include/mpp_util_sma.inc
shared/mpp/include/mpp_write.h
shared/mpp/include/mpp_write_2Ddecomp.h
shared/mpp/include/system_clock.h
shared/oda_tools/oda_core.F90
shared/oda_tools/oda_types.F90
shared/oda_tools/write_ocean_data.F90
shared/oda_tools/xbt_drop_rate_adjust.f90
shared/platform/platform.F90
shared/random_numbers/MersenneTwister.F90
shared/random_numbers/random_numbers.F90
shared/sat_vapor_pres/sat_vapor_pres.F90
shared/sat_vapor_pres/sat_vapor_pres_k.F90
shared/station_data/station_data.F90
shared/time_interp/time_interp.F90
shared/time_interp/time_interp_external.F90
shared/time_manager/get_cal_time.F90
shared/time_manager/time_manager.F90
shared/topography/gaussian_topog.F90
shared/topography/topography.F90
shared/tracer_manager/tracer_manager.F90
shared/tridiagonal/tridiagonal.F90


EOF

set srcList = ( mom4p1/ocean_bgc mom4p1/ocean_core mom4p1/ocean_diag mom4p1/ocean_param/sources mom4p1/ocean_param/mixing mom4p1/ocean_param/gotm-4.0/include mom4p1/ocean_param/gotm-4.0/turbulence mom4p1/ocean_param/gotm-4.0/util mom4p1/ocean_tracers  ocean_shared/generic_tracers  atmos_null coupler ice_sis ice_param land_null )


# compile the model code and create executable
set makeFile      = Make_$type
  cd $executable:h
  $mkmf -f -m $makeFile -a $code_dir -t $mkmfTemplate -p $executable:t -c "$cppDefs" $srcList $pathnames $root/include $code_dir/shared/include $code_dir/shared/mpp/include 
#/usr/local/include

  make -f $makeFile

