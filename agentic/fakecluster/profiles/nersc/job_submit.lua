-- Site submit policy: every job names its node type (-C cpu or -C gpu) and EITHER a QOS or a partition name, never
-- both. The name is the QOS (debug, regular or shared; regular when neither is given), and the QOS with the node type
-- picks the partition the job runs in.
local QOS = { debug = true, regular = true, shared = true }
-- QOS the site has since retired, one name per line (none by default): a job naming one is refused like any other.
local RETIRED = "/etc/slurm/retired_qos.conf"

local function retired(q)
  local f = io.open(RETIRED, "r")
  if f == nil then
    return false
  end
  for line in f:lines() do
    if line == q then
      f:close()
      return true
    end
  end
  f:close()
  return false
end

local function refuse()
  slurm.log_user("Job request does not match any supported policy.")
  return slurm.ERROR
end

function slurm_job_submit(job_desc, part_list, submit_uid)
  local features = job_desc.features or ""
  local cpu = string.find(features, "cpu", 1, true) ~= nil
  local gpu = string.find(features, "gpu", 1, true) ~= nil
  if not cpu and not gpu then
    return refuse()
  end
  if job_desc.partition ~= nil and job_desc.qos ~= nil then
    return refuse()
  end
  local q = job_desc.qos or job_desc.partition or "regular"
  if not QOS[q] or retired(q) then
    return refuse()
  end
  job_desc.qos = q
  if gpu then
    job_desc.partition = "gpu_ss11"
  elseif q == "shared" then
    job_desc.partition = "shared_milan_ss11"
  else
    job_desc.partition = "regular_milan_ss11"
  end
  return slurm.SUCCESS
end

function slurm_job_modify(job_desc, job_rec, part_list, modify_uid)
  return slurm.SUCCESS
end
