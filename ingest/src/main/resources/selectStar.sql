select
t.path path,
t.exit_value tk_exit,
t.stderr tk_stderr,
pi.stderr pi_stderr,
pi.stdout pi_stdout,
pi.exit_value pi_exit,
mc.stderr mc_stderr,
mc.stdout mc_stdout,
mc.exit_value mc_exit,
mt.stderr mt_stderr,
mt.stdout mt_stdout,
mt.exit_value mt_exit,
q.stderr q_stderr,
q.exit_value q_exit

from tika t
join mutoolclean mc on mc.path=t.path
join mutooltext mt on mt.path = t.path
join pdfinfo pi on pi.path = t.path
join qpdf q on q.path = t.path
