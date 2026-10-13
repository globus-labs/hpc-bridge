"""The shared corpus for recognising a scheduler job submitted BY HAND (2026-10-10).

Two detectors read it: the product's `scheduler_ops._scheduler_submission` (a lexer; the notice on run_shell and
login_shell) and the harness grader's `invariants._scheduler_submits` (an independent skeleton + token walk; the spend
graders). `tests/test_server.py` holds the product to it; `agentic/harness/test_invariants.py` holds the grader to it
and checks the two agree. Many entries are commands agents actually ran (stored harness bundles) or the review's
counter-examples.

Each CORPUS row is (command, inside_job, expected): `inside_job` — it runs inside a compute block, where `srun` is a
step of the block's own job; `expected` — the scheduler command that starts a NEW job, or None.
"""
from __future__ import annotations

CORPUS: list[tuple[str, bool, str | None]] = [
    # --- submissions, as agents wrote them
    ("cd ~/hpcb_longjob && sbatch run.sbatch", False, "sbatch"),                        # long_job_30m, live
    ("rm -f p.log; cd ~/x && sbatch --parsable job.sh", False, "sbatch"),
    ("jid=$(sbatch --parsable job.sh); echo $jid", False, "sbatch"),
    ('echo "submitted $(sbatch --parsable job.sh)"', False, "sbatch"),
    ("x=`sbatch job.sh`", False, "sbatch"),
    ('x="`sbatch job.sh`"', False, "sbatch"),
    ("cd /tmp && printf '#!/bin/bash\\nhostname\\n' > t.pbs && qsub -q debug t.pbs; echo rc=$?", False, "qsub"),
    ("qsub -q debug < /tmp/r.pbs", False, "qsub"),
    ("for A in a b; do printf '#!/bin/bash\\nsleep 5\\n' | qsub -q debug -A \"$A\" -l select=1; done", False, "qsub"),
    ("qsub -h job.pbs", False, "qsub"),                         # PBS -h HOLDS the job it still submits
    ("qsub -I -l select=1", False, "qsub"),
    ("qsub -- /bin/hostname --version", False, "qsub"),         # after `--` it is the job's own command line
    ("srun -N1 -p debug hostname", False, "srun"),
    ("srun --pty bash", False, "srun"),
    ("srun hostname | sort", False, "srun"),
    ("nohup srun -N1 ./long > out 2>&1 &", False, "srun"),
    ("salloc -N1 --time=10", False, "salloc"),
    ("salloc -N1 srun python --version", False, "salloc"),
    ("/usr/bin/sbatch job.sh", False, "sbatch"),
    ("SBATCH_ACCOUNT=p1 sbatch job.sh", False, "sbatch"),
    ("cd /x && SBATCH_PARTITION=p sbatch j.sh", False, "sbatch"),
    ("module load slurm && sbatch job.sh", False, "sbatch"),
    ("if sbatch job.sh; then echo ok; fi", False, "sbatch"),
    ("until sbatch x; do sleep 1; done", False, "sbatch"),
    ("for f in *.sh; do sbatch $f; done", False, "sbatch"),
    ("while read f; do sbatch $f; done < list", False, "sbatch"),
    ("{ sbatch x; }", False, "sbatch"),
    ("(sbatch job.sh)", False, "sbatch"),
    ("true && sbatch job.sh", False, "sbatch"),
    ("sbatch job.sh &", False, "sbatch"),
    ("sleep 1 & sbatch job.sh", False, "sbatch"),
    ("echo a >&2 && sbatch x", False, "sbatch"),
    ("cmd &>log; sbatch x", False, "sbatch"),
    ("case $s in slurm) sbatch job.sh;; pbs) qsub job.pbs;; esac", False, "sbatch"),
    ("case $s in\n  pbs) echo pbs ;;\n  slurm) sbatch job.sh ;;\nesac", False, "sbatch"),
    ("x=$(echo \")\"); sbatch job.sh", False, "sbatch"),          # a `)` in quotes does not close the substitution
    ("x=$(printf '%s' 'a)b'); sbatch job.sh", False, "sbatch"),
    ("sbatch \\\n  --time=10 job.sh", False, "sbatch"),
    ("sbatch <<EOF\n#!/bin/bash\nhostname\nEOF", False, "sbatch"),
    ("sbatch <<< '#!/bin/bash\nhostname'", False, "sbatch"),
    ("cat > job.sh <<'EOF'\n#!/bin/bash\n#SBATCH -p debug\nsrun python sim.py\nEOF\nsbatch job.sh", False, "sbatch"),
    ("echo 'it'\"'\"'s'; sbatch x", False, "sbatch"),
    ('echo "a\\"b"; sbatch x', False, "sbatch"),
    ("echo ${#arr[@]}; sbatch x", False, "sbatch"),
    ('"sbatch" job.sh', False, "sbatch"),                       # a quoted command name is still the command
    ("'sbatch' job.sh", False, "sbatch"),
    ("2>/dev/null sbatch job.sh", False, "sbatch"),             # a redirection may come first
    (">log sbatch job.sh", False, "sbatch"),
    # --- wrappers, with their own arguments
    ("SBATCH_ACCOUNT=p1 timeout 60 sbatch job.sh", False, "sbatch"),
    ("timeout -s KILL 60 sbatch job.sh", False, "sbatch"),
    ("nice -n 10 sbatch j.sh", False, "sbatch"),
    ("stdbuf -oL sbatch j.sh", False, "sbatch"),
    ("time sbatch job.sh", False, "sbatch"),
    ("time -p sbatch j.sh", False, "sbatch"),
    ("nohup sbatch job.sh", False, "sbatch"),
    ("setsid sbatch j.sh", False, "sbatch"),
    ("setsid -f sbatch j.sh", False, "sbatch"),
    ("env VAR=1 sbatch job.sh", False, "sbatch"),
    ("env -u FOO sbatch j.sh", False, "sbatch"),
    ("env -i PATH=/usr/bin sbatch j.sh", False, "sbatch"),
    ("echo job.sh | xargs sbatch", False, "sbatch"),
    ("ls *.sh | xargs -n1 sbatch", False, "sbatch"),
    ("xargs -a list -n1 sbatch", False, "sbatch"),
    ("sudo sbatch job.sh", False, "sbatch"),
    ("exec sbatch x", False, "sbatch"),
    ("command sbatch x", False, "sbatch"),
    ("command sbatch -v job.sh", False, "sbatch"),               # sbatch's own -v (verbose), not command -v
    ("bash -lc 'module load slurm; sbatch job.sh'", False, "sbatch"),
    ('sh -c "sbatch $J"', False, "sbatch"),
    ("eval 'sbatch job.sh'", False, "sbatch"),
    ("find . -name '*.sh' -exec sbatch {} \\;", False, "sbatch"),
    # --- the program's own options are not the scheduler's: options are read only before the first positional
    ("srun -N1 python -V", False, "srun"),
    ("srun -N1 python --version", False, "srun"),
    ("srun -p gpu --gres=gpu:1 nvcc -V", False, "srun"),
    ("srun -n1 mpirun --version", False, "srun"),
    ("srun -N1 hostname -h", False, "srun"),
    ("sbatch job.sh --help", False, "sbatch"),
    ("sbatch job.sh --test-only", False, "sbatch"),
    ("sbatch --wrap='python sim.py --version'", False, "sbatch"),
    # --- a here-document operator inside a string or a comment hides nothing
    ('echo "use cat <<EOF to write"\nsbatch job.sh', False, "sbatch"),
    ("# cat <<EOF\nsbatch job.sh", False, "sbatch"),
    ('printf "%s\\n" "<<EOF"\nsbatch job.sh', False, "sbatch"),
    ("echo $((1<<x))\nsbatch job.sh", False, "sbatch"),
    ("x=$((1 <<EOF))\nsbatch job.sh", False, "sbatch"),
    # --- inside a compute block: sbatch/qsub/salloc start a NEW job beside it; srun is a step of the block's own job
    ("sbatch -t 48:00:00 long.sh", True, "sbatch"),
    ("cd run && qsub -q long job.pbs", True, "qsub"),
    ("salloc -N2 ./mpi_app", True, "salloc"),
    ("srun -n 4 ./mpi_app", True, None),
    ("srun hostname", True, None),
    ("srun --jobid=123 --overlap nvidia-smi", False, None),    # a step in an allocation that already exists
    ("srun --jobid 123 hostname", False, None),
    # --- mentions, queries and job scripts being WRITTEN: not submissions
    ("squeue -u $USER -h -o '%i|%P|%T|%r'", False, None),
    ("sacct -X", False, None),
    ("scancel 123; qdel 456; qstat -u $USER; sacct -X -n", False, None),
    ("scontrol show job 1", False, None),
    ("strigger --set", False, None),
    ("watch -n 5 squeue", False, None),
    ("man sbatch", False, None),
    ("sbatch --help", False, None),
    ("sbatch -h", False, None),
    ("sbatch -V", False, None),
    ("sbatch --test-only job.sh", False, None),
    ("srun --version", False, None),
    ("srun --usage", False, None),
    ("salloc -h", False, None),
    ("salloc --version", False, None),
    ("qsub --version", False, None),
    ("echo sbatch", False, None),
    ("which sbatch", False, None),
    ("alias sb=sbatch", False, None),
    ("builtin echo sbatch", False, None),
    ("command -v sbatch && command -v squeue && command -v scancel", False, None),
    ("which sinfo sbatch squeue sacctmgr 2>/dev/null", False, None),
    ("grep sbatch x", False, None),
    ("grep -iE 'error|sbatch|submit|reject' ~/.globus_compute/uep.x/endpoint.log | tail -30", False, None),
    ('grep -hE "^#SBATCH" $d/submit_scripts/* 2>/dev/null', False, None),
    ("# sbatch job.sh", False, None),
    ("echo hi  # then sbatch job.sh", False, None),
    ("echo hi#sbatch", False, None),
    ('echo "next: sbatch job.sh"', False, None),
    ('echo "step 2; sbatch job.sh" >> plan.txt', False, None),   # a separator INSIDE quotes separates nothing
    ("echo $(date) sbatch", False, None),                          # after a substitution's `)`: still an argument
    ("echo `date` sbatch", False, None),
    ("bash -c 'echo sbatch'", False, None),
    ("echo a \\\n sbatch x", False, None),                         # a line continuation: one command
    ("cat > job.sh <<'SHEOF'\n#!/bin/bash\n#SBATCH --time=00:35:00\necho \"sbatch job $SLURM_JOB_ID\"\nsrun ./a.out\n"
     "SHEOF\nchmod +x job.sh; cat job.sh", False, None),            # a job script WRITTEN, not submitted
    ("cat <<EOF > job.sh\n#!/bin/bash\nsrun ./a\nEOF", False, None),
    ("cat > j.sh <<-EOF\n\tsrun ./a.out\n\tEOF\necho written", False, None),
    ('submit() { sbatch "$1"; }', False, None),                   # a function DEFINED, not called
    ("function submit { sbatch \"$1\"; }", False, None),
    ('submit() {\n  sbatch "$1"\n}', False, None),                # … over several lines
    ('function submit {\n  cd run\n  sbatch "$1"\n}', False, None),
    ("ls ~/hpcb/run.sbatch; cat job.sbatch", False, None),
    ("if command -v sbatch >/dev/null 2>&1; then echo SCHED=slurm; elif command -v qsub >/dev/null 2>&1; "
     "then echo SCHED=pbs; fi", False, None),                     # the discovery probe
    ("for s in sbatch qsub; do which $s; done", False, None),
    ('case "$(command -v sbatch qsub)" in */sbatch) echo slurm;; */qsub) echo pbs;; esac', False, None),  # patterns
    ("command -p -v sbatch", False, None),
    ("echo $SLURM_JOB_ID 2>&1 | tee log", False, None),
]

# Submissions BOTH detectors miss, by design (precision first): behind a variable, inside another language, over ssh,
# or in a function called later. Each row: (command, inside_job).
KNOWN_LIMITS: list[tuple[str, bool]] = [
    ("SB=sbatch; $SB job.sh", False),
    ("python -c 'import subprocess; subprocess.run([\"sbatch\", \"j\"])'", False),
    ("ssh host sbatch job.sh", False),
    ("f() { sbatch job.sh; }; f", False),
    ("bash run_all.sh", False),                                    # a script that submits inside
    ("watch -n 60 sbatch job.sh", False),                          # wrappers neither detector knows
    ("ionice -c3 sbatch job.sh", False),
    ("parallel sbatch ::: a.sh b.sh", False),
]

# Commands on which the two detectors are allowed to disagree, and why. Empty: they agree on the whole corpus.
KNOWN_DIFFERENCES: dict[str, str] = {}
