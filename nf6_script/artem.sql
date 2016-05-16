SELECT fam, rost
                FROM
                    --Таблцица с fam
                    (select a.id_stud,fam
                       from (
                               select id_stud, dat,fam, abs(dat -d) as div
                               from stud_fam
                            ) as a
                       inner join
                            (
                             select id_stud, min( abs(d-dat)) as mindiv
                             from stud_fam
                             where dat <= d
                             group by id_stud
                            ) as b
                       on a.id_stud= b.id_stud and a.div = b.mindiv) as fam
                    inner join
                    --Таблцица с rost
                    (select a.id_stud,rost
                       from (
                               select id_stud, dat,rost, abs(dat -d) as div
                               from  stud_rost
                            ) as a
                       inner join
                            (
                             select id_stud, min( abs(d-dat)) as mindiv
                             from  stud_rost
                             where dat <= d
                             group by id_stud
                            ) as b
                       on a.id_stud= b.id_stud and a.div = b.mindiv) as rost
                    on fam.id_stud = rost.id_stud;
                           