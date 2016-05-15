SELECT nam, fam, otc
                FROM
                    --Таблцица с nam
                    (select a.id_stud,nam
                       from (
                               select id_stud, dat,nam, abs(dat -d) as div
                               from stud_nam
                            ) as a
                       inner join
                            (
                             select id_stud, min( abs(d-dat)) as mindiv
                             from stud_nam
                             where dat <= d
                             group by id_stud
                            ) as b
                       on a.id_stud= b.id_stud and a.div = b.mindiv) as nam
                    inner join
                    --Таблцица с fam
                    (select a.id_stud,fam
                       from (
                               select id_stud, dat,fam, abs(dat -d) as div
                               from  stud_fam
                            ) as a
                       inner join
                            (
                             select id_stud, min( abs(d-dat)) as mindiv
                             from  stud_fam
                             where dat <= d
                             group by id_stud
                            ) as b
                       on a.id_stud= b.id_stud and a.div = b.mindiv) as fam
                    on nam.id_stud = fam.id_stud
                       inner join
                       
                    --Таблцица с otc
                    (select a.id_stud,otc
                       from (
                               select id_stud, dat,otc, abs(dat -d) as div
                               from  stud_otc
                            ) as a
                       inner join
                            (
                             select id_stud, min( abs(d-dat)) as mindiv
                             from  stud_otc
                             where dat <= d
                             group by id_stud
                            ) as b
                       on a.id_stud= b.id_stud and a.div = b.mindiv) as otc
                    on fam.id_stud = otc.id_stud;
                           