select
p.path as fname,
p.size,
p.shasum256 as shasum_256,
p.collection,
a.timeout as arlington_timeout,
a.exit_value as arlington_exit_value,
mc.stderr mc_warn,
mc.stdout mc_stdout,
mc.exit_value mc_exit,
case
    when mc.stderr like 'error: cannot authenticate password%' then 'encrypted'
	when mc.path is null then 'missing'
	when mc.timeout=true then 'timeout'
	when mc.exit_value <> 0 then 'crash'
	when length(mc.stderr) > 5 then 'warn'
	else 'success'
end as mc_status,
mt.stderr mt_warn,
mt.stdout mt_stdout,
mt.exit_value mt_exit,
case
    when mt.stderr like 'error: cannot authenticate password%' then 'encrypted'
    when mt.path is null then 'missing'
	when mt.timeout=true then 'timeout'
	when mt.exit_value <> 0 then 'crash'
	when length(mt.stderr) > 5 then 'warn'
	else 'success'
end as mt_status,
case
    when pc.stderr like 'error: cannot authenticate password%' then 'encrypted'
    when pc.path is null then 'missing'
	when pc.timeout=true then 'timeout'
	when pc.exit_value <> 0 then 'crash'
	when length(pc.stderr) > 5 then 'warn'
	else 'success'
end as pc_status,
case
    when cpu.stderr like 'pdfcpu: please provide the correct password%' then 'encrypted'
    when cpu.path is null then 'missing'
    when cpu.timeout=true then 'timeout'
    when cpu.exit_value <> 0 then 'crash'
    when length(cpu.stderr) > 5 then 'warn'
	else 'success'
end as cpu_status,
cpu.stderr as cpu_warn,
pid.stderr pid_stderr,
pid.stdout pid_stdout,
pid.exit_value pid_exit,
case
    when pid.stdout ~ '/Encrypt [1-9]' then 'encrypted'
	when pid.path is null then 'missing'
	when pid.timeout=true then 'timeout'
	when pid.exit_value <> 0 then 'crash'
	when length(pid.stderr) > 5 then 'warn'
	else 'success'
end as pid_status,
pim.stderr pim_stderr,
pim.stdout pim_stdout,
pim.exit_value pim_exit,
case
    when pim.stderr like 'Command Line Error: Incorrect password%' then 'encrypted'
	when pim.path is null then 'missing'
	when pim.timeout=true then 'timeout'
	when pim.exit_value <> 0 then 'crash'
	when length(pim.stderr) > 5 then 'warn'
	else 'success'
end as pim_status,
pinfo.stderr pinfo_stderr,
pinfo.stdout pinfo_stdout,
pinfo.exit_value pinfo_exit,
case
    when pinfo.stderr like 'Command Line Error: Incorrect password%' then 'encrypted'
    when pinfo.path is null then 'missing'
	when pinfo.timeout=true then 'timeout'
	when pinfo.exit_value <> 0 then 'crash'
	when length(pinfo.stderr) > 5 then 'warn'
	else 'success'
end as pinfo_status,
pmd.stderr pmd_warn,
pmd.stdout pmd_stdout,
pmd.exit_value pmd_exit,
case
    when pmd.stderr like '%raise PDFPasswordIncorrect%' then 'encrypted'
    when pmd.path is null then 'missing'
	when pmd.timeout=true then 'timeout'
	when pmd.exit_value <> 0 then 'crash'
	when length(pmd.stderr) > 5 then 'warn'
	else 'success'
end as pmd_status,
pmt.stderr pmt_warn,
pmt.stdout pmt_stdout,
pmt.exit_value pmt_exit,
case
    when pmd.stderr like '%raise PDFPasswordIncorrect%' then 'encrypted'
    when pmt.path is null then 'missing'
	when pmt.timeout=true then 'timeout'
	when pmt.exit_value <> 0 then 'crash'
	when length(pmt.stderr) > 5 then 'warn'
	else 'success'
end as pmt_status,
ppm.stderr ppm_stderr,
ppm.stdout ppm_stdout,
ppm.exit_value ppm_exit,
case
    when ppm.stderr like 'Command Line Error: Incorrect password%' then 'encrypted'
    when ppm.path is null then 'missing'
	when ppm.timeout=true then 'timeout'
	when ppm.exit_value <> 0 then 'crash'
	when length(ppm.stderr) > 5 then 'warn'
	else 'success'
end as ppm_status,
ps.stderr ps_stderr,
ps.stdout ps_stdout,
ps.exit_value ps_exit,
case
    when ps.stderr like 'Command Line Error: Incorrect password%' then 'encrypted'
    when ps.path is null then 'missing'
	when ps.timeout=true then 'timeout'
	when ps.exit_value <> 0 then 'crash'
	when length(ps.stderr) > 5 then 'warn'
	else 'success'
end as ps_status,
ptt.stderr ptt_stderr,
ptt.exit_value ptt_exit,
case
    when ptt.stderr like 'Incorrect password%' then 'encrypted'
    when ptt.path is null then 'missing'
	when ptt.timeout=true then 'timeout'
	when ptt.exit_value <> 0 then 'crash'
	when length(ptt.stderr) > 5 then 'warn'
	else 'success'
end as ptt_status,


case
    when t.stderr like 'org.apache.tika.exception.EncryptedDocumentException%' then 'encrypted'
    when t.path is null then 'missing'
	when t.timeout=true then 'timeout'
	when t.exit_value <> 0 then 'crash'
	when length(t.stderr) > 5 then 'warn'
	else 'success'
end as tk_status,
t.exit_value tk_exit,
t.stderr tk_stderr,
q.stderr q_stderr,
q.exit_value q_exit,
case
    when q.path is null then 'missing'
	when q.timeout=true then 'timeout'
	when q.exit_value <> 0 then 'crash'
	when length(q.stderr) > 5 then 'warn'
	else 'success'
end as q_status,
c.stdout clamav,
c.stderr c_stderr,
c.exit_value c_exit,
case
    when c.path is null then 'missing'
	when c.timeout=true then 'timeout'
	when c.exit_value <> 0 then 'crash'
	when length(c.stderr) > 5 then 'warn'
	else 'success'
end as c_status,
cd.stdout cd,
cd.stderr cd_warn,
cd.exit_value cd_exit,
case
    when cd.path is null then 'missing'
	when cd.timeout=true then 'timeout'
	when cd.exit_value <> 0 then 'crash'
	when length(cd.stderr) > 5 then 'warn'
	else 'success'
end as cd_status

--if using itext encrypted: com.itextpdf.text.exceptions.BadPasswordException
from profiles p
left join arlington a on a.path = p.path
left join mutoolclean mc on mc.path=p.path
left join mutooltext mt on mt.path = p.path
left join pdfchecker pc on pc.path = p.path
left join pdfcpu cpu on cpu.path = p.path
left join pdfid pid on pid.path = p.path
left join pdfimages pim on pim.path = p.path
left join pdfinfo pinfo on pinfo.path = p.path
left join pdfminerdump pmd on pmd.path = p.path
left join pdfminertext pmt on pmt.path = p.path
left join pdftoppm ppm on ppm.path = p.path
left join pdftops ps on ps.path = p.path
left join pdftotext ptt on ptt.path = p.path
left join qpdf q on q.path = p.path
left join tika t on t.path = p.path
left join clamav c on c.path = p.path
left join caradoc cd on cd.path = p.path

