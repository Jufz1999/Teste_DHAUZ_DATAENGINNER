#Primeira etapa de qualquer utilização de base é conhece-la melhor, entender quais variaveis tem disponiveis e como estão os dados por isso visão geral:
select distinct 
				*
from db_hiring_test.raw_transactions_table;

#query para verificar dados mais recentes
select distinct 
				ImportDate
from db_hiring_test.raw_transactions_table;

#query para verificar status das transações
select distinct 
                 Status 
from db_hiring_test.raw_transactions_table;

#query para verificar sentdate
select distinct
                sentdate 
from db_hiring_test.raw_transactions_table;
#query para verificar consistencias de origem e destino de transações
select distinct idtransaction
               ,addressorigin
               ,addressdestination 
from db_hiring_test.raw_transactions_table
	where addressorigin = addressdestination;

#------------------------------------------------Etapa 1-----------------------------------------------------------
#Questão 1
##Resposta: o endereço com maior numero de confirmada transações é a A-99 com 45 transações confirmadas
select distinct AddressOrigin
				,count(distinct IdTransaction) as count_transaction
from db_hiring_test.raw_transactions_table
				where ImportDate= '2021-02-05 23:59:59' #Dados mais recentes 
				and Status = 'Confirmed' #Transações confirmadas
				and addressorigin != addressdestination #Transações coerentes com origem e destino diferentes 
				and addressorigin != '' #Transações com origem
				and  addressdestination !='' #Transações com destino
group by AddressOrigin 
order by 2 desc;

#Questão 2
#Resposta: O dia do mês com maior numero de transaçõe confirmadas foi dia 02 com 161.
select distinct SUBSTRING(sentdate,9,2) as Date
				,count(distinct IdTransaction)as count_transaction
from db_hiring_test.raw_transactions_table
		where ImportDate= '2021-02-05 23:59:59' #Dados mais recentes 
		and Status = 'Confirmed' #Transações confirmadas
		and addressorigin != addressdestination #Transações com origem e destino diferentes
		and addressorigin != '' #Transações com origem 
		and  addressdestination !='' #Transações com destino
group by SUBSTRING(sentdate,9,2)
order by 2 desc;

#Questão 3
#resposta: O dia da semana com maior numero de transações é sexta feira, o que no mysql vem como o dia 4.

select distinct count(distinct idtransaction) as count_transaction
				,week_day,number_day_week
from    	
					(select distinct *,
					                case when nume_dia_semana = 0 then 'segunda' 
					                     when nume_dia_semana = 1 then 'terça'
										 when nume_dia_semana = 2 then 'quarta'
										 when nume_dia_semana = 3 then 'quinta'
										 when nume_dia_semana = 4 then 'sexta'
										 when nume_dia_semana = 5 then 'sabado'
										 when nume_dia_semana = 6 then 'domingo'
														else null end as week_day
 from 
              (Select *
                     ,weekday(substring(Sentdate,1,10)) as number_day_week
               from db_hiring_test.raw_transactions_table as tra) as a) as b;

#Questão 4
#Resposta: As transações abaixo necessitam ser revistas com a equipe responsavel por estarem com o mesmo endereço de origem e destino.
select distinct idtransaction
				,addressorigin
                ,addressdestination 
from db_hiring_test.raw_transactions_table
		where addressorigin = addressdestination
		or(addressorigin = ''
		or addressdestination = '');

#Questão 5
#Resposta: A carteira com maior receita é a-83.
Select distinct addressorigin
				,sum(totalsent) as total 
from db_hiring_test.raw_transactions_table
		where ImportDate= '2021-02-05 23:59:59' #dados mais recentes 
		and Status = 'Confirmed' #Transações confirmadas
		and addressorigin != addressdestination #Transações com origem e destino diferentes
		and addressorigin != '' #Transações com origem
		and  addressdestination !='' #Transações com destino
group by addressorigin
order by total desc;

#-----------------------------------------------Etapa 2---------------------------------------------------------#

#Analise de base 
select distinct 
	* 
from db_hiring_test.raw_transactions_fee;

#Unindo as tabelas através do Join das tabelas
select *,
	  ((`fee-percentage`)/100)*replace(totalsent,',','') as taxa
from db_hiring_test.raw_transactions_table as tra
		inner join db_hiring_test.raw_transactions_fee as taxa
			on cast((replace(tra.totalsent,',','')) as float) > cast(`range-start`as float)
			and cast((replace(tra.totalsent,',','')) as float) < cast(`range-end`as float)
			and ImportDate = '2021-02-05 23:59:59' #dados mais recentes 
			and Status = 'Confirmed' #Transações confirmadas 

#Criando a tabela temporária
CREATE TEMPORARY TABLE db_hiring_test.TempJoin
                           select * 
                                  ,((`fee-percentage`)/100)*replace(totalsent,',','') as taxa
						   from db_hiring_test.raw_transactions_table as tra
									inner join db_hiring_test.raw_transactions_fee as taxa
										on cast((replace(tra.totalsent,',','')) as float) > cast(`range-start`as float)
										and cast((replace(tra.totalsent,',','')) as float) < cast(`range-end`as float)
										and ImportDate = '2021-02-05 23:59:59' #dados mais recentes 
										and Status = 'Confirmed' #Transações confirmadas 
											;

#Questão 1
#Resposta: A carteira que mais pagou taxa em janeiro de 2021 foi a A-99, e maior pagamento de percentual seria A-62.
##Calculo de maior carteira pagadora de percentual.
select distinct addressorigin
				, sum(`fee-percentage`) as soma_taxa 
from db_hiring_test.TempJoin
	where sentdate like '2021-01%' #filtro para apenas janeiro de 2021
group by addressorigin
order by 2 desc;

##Calculo de maior carteira pagadora de valor como taxa
select distinct addressorigin
				,sum(taxa) as taxa 
from db_hiring_test.TempJoin
	where sentdate like '2021-01%' #filtro para apenas janeiro de 2021
group by addressorigin
order by 2 desc;

#Questão 2
#Resposta: A carteira que mais pagou percentual de taxa em fevereiro de 2021 foi a A-82, e a de valor bruto em taxa foi A-29.
##Calculo de maior carteira pagadora de percentual.
select distinct addressorigin
				,sum(`fee-percentage`) as percentual 
from db_hiring_test.TempJoin
	where sentdate like '2021-02%' #filtro para apenas fevereiro de 2021
group by addressorigin
order by 2 desc;

##Calculo de maior carteira pagadora de valor de taxa 
select distinct addressorigin
				,sum(taxa) as taxa 
from db_hiring_test.TempJoin
	where sentdate like '2021-02%'#filtro para apenas fevereiro de 2021
group by addressorigin
order by 2 desc;

#Questão 3
#Resposta: A carteira que mais pagou percentual taxa  foi a A-82 e a que mais pagou em valor de taxa foi A-99
##Calculo de maior carteira pagadora de percentual.
select distinct addressorigin
				,sum(`fee-percentage`) as percentual 
from db_hiring_test.TempJoin
group by addressorigin
order by 2 desc;

##Calculo de maior carteira pagadora de valor em taxa
select distinct addressorigin
				,sum(taxa)as taxa
                ,sum(`fee-percentage`) as percentual 
from db_hiring_test.TempJoin
group by addressorigin
order by 2 desc;
					
#Questão 4 
# Resposta: A média de valor de taxa é R$2.1745,80 e o percentual médio é 5,88%

#Média de percentual e valor de taxa 
select distinct round(avg(taxa),2)as taxa
				,round(avg(`fee-percentage`),2) as percentual
from db_hiring_test.TempJoin;





