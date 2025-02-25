#!/usr/bin/env bash
# Gao Wang and MemVerge Inc.

set -o errexit -o nounset -o pipefail

dryrun=""
env_parameters=""
image_vol_size=""
root_vol_size=""
publish=""
declare -a float_args=()

while (( "$#" )); do
  case "$1" in
        --float-executable) float_executable="$2"; shift 2;;
        # Basic parameters
        --core) core="$2"; shift 2;;
        --gateway) gateway="$2"; shift 2;;
        --image) image="$2"; shift 2;;
        --mem) mem="$2"; shift 2;;
        --opcenter) opcenter="$2"; shift 2;;
        --publish) publish="$2"; publish="$2"; shift 2;;
        --securityGroup) securityGroup="$2"; shift 2;;
        --vmPolicy) vm_policy="$2"; shift 2;;
        # Volume parameters
        --dataVolumeOption) dataVolumeOption="$2"; shift 2;;
        --imageVolSize) image_vol_size="$2"; shift 2;;
        --rootVolSize) root_vol_size="$2"; shift 2;;
        #Script parameters
        --host-script) host_script="$2"; shift 2;;
        --job-script) job_script="$2"; shift 2;;
        # Miscellaneous parameters
        --dryrun) dryrun=true; shift ;;
        --extra_parameters) extra_parameters="$2"; shift 2;;
        --env_parameters) env_parameters="$2"; shift 2;;
        --job-name) job_name="$2"; shift 2;;
        *) echo "Unknown parameter passed: $1"; usage; exit 1 ;;
    esac
done

float_args+=(
    "-a" "$opcenter"
    "-i" "$image" "-c" "$core" "-m" "$mem"
    "--vmPolicy" "$vm_policy"
    "--gateway" "$gateway"
    "--securityGroup" "$securityGroup"
    "--migratePolicy" "[disable=true,evadeOOM=false]"
    "--withRoot"
    "--allowList" "[r5*,r6*,r7*,m*]"
    "-j" "$job_script"
    "--hostInit" "${host_script}"
    "--dirMap" "/mnt/efs:/mnt/efs"
    "-n" "$job_name"
    "${env_parameters}"
)

# If dataVolume is nonempty, add in mount and ebs mounts
if (( ${#dataVolumeOption[@]} )); then
    float_args+=(
        "${dataVolumeOption[@]}"
    )
fi

# If image vol size and root vol size not empty, populate float args
if [[ -n  "$image_vol_size" ]]; then
    float_args+=(
        "--imageVolSize" "$image_vol_size"
    )
fi

if [[ -n  "$root_vol_size" ]]; then
    float_args+=(
        "--rootVolSize" "$root_vol_size"
    )
fi

if [[ -n "$publish" ]]; then
    float_args+=(
        "--publish" "$publish"
    )
fi

float_args+=(
    "$extra_parameters"
)

echo ""
echo "#-------------"
echo "${float_executable} submit ${float_args[*]}"
echo "#-------------"

if [[ ${dryrun} == true ]]; then
    echo "Command not submitted because dryrun was requested."
    exit 0
else
    echo "yes" | "${float_executable} submit ${float_args[*]}"
fi
