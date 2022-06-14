#!/usr/bin/env python
# coding: utf-8

# In[1]:


##baixando pacote do mysql
get_ipython().system('pip install mysql-connector-python==8.0.28')


# In[23]:


##importando bibliotecas 
import mysql.connector
import sys
import pandas as pd
import pymysql
from sqlalchemy import create_engine


# In[3]:


## declarando variaveis para conexão com o banco de dados
db_password = "D3@bGh664%$1VHv*"
ENDPOINT  ="dhauz-instance.cutloqirhpd7.us-east-1.rds.amazonaws.com"
USER = "candidate_user"
token = db_password
DBNAME = "db_hiring_test"


# In[45]:


##conexão com o banco de dados  e criando cursor 
cnx = pymysql.connect(host=ENDPOINT, user=USER, password=token, port=3306, database=DBNAME) ##conexão
cursor = cnx.cursor()





# In[25]:


#Primeira etapa de qualquer utilização de base é conhece-la melhor, entender quais variaveis tem disponiveis e como estão os dados por isso visão geral:

tabela_valid = pd.read_sql_query("""select distinct 
				*
from db_hiring_test.raw_transactions_table;""",cnx)

tabela_valid.head()


# In[26]:


#query para verificar dados mais recentes

tabela_valid_impordate = pd.read_sql_query("""select distinct 
				ImportDate
from db_hiring_test.raw_transactions_table;""",cnx)

tabela_valid_importdate.head()


# In[27]:


#query para verificar status das transações

tabela_valid_status = pd.read_sql_query("""select distinct 
                 Status 
from db_hiring_test.raw_transactions_table;""",cnx)

tabela_valid_status.head()


# In[33]:


#query para verificar sentdate

tabela_valid_sentdate = pd.read_sql_query("""select distinct
                sentdate 
from db_hiring_test.raw_transactions_table
where sentdate is null;""",cnx)

tabela_valid_status.head()


# In[31]:


#query para verificar consistencias de origem e destino de transações
tabela_valid_cons_transaction = pd.read_sql_query("""select distinct idtransaction
               ,addressorigin
               ,addressdestination 
from db_hiring_test.raw_transactions_table
	where addressorigin = addressdestination;""",cnx)

tabela_valid_cons_transaction.head()


# In[32]:


## ETAPA 1
#Questão 1
##Resposta: o endereço com maior numero de confirmada transações é a A-99 com 45 transações confirmadas

question_1 = pd.read_sql_query("""select distinct AddressOrigin
				,count(distinct IdTransaction) as count_transaction
from db_hiring_test.raw_transactions_table
				where ImportDate= '2021-02-05 23:59:59' #Dados mais recentes 
				and Status = 'Confirmed' #Transações confirmadas
				and addressorigin != addressdestination #Transações coerentes com origem e destino diferentes 
				and addressorigin != '' #Transações com origem
				and  addressdestination !='' #Transações com destino
group by AddressOrigin 
order by 2 desc;""",cnx)

question_1.head()


# In[34]:


#Questão 2
#Resposta: O dia do mês com maior numero de transaçõe confirmadas foi dia 02 com 161.

question_2 = pd.read_sql_query("""select distinct SUBSTRING(sentdate,9,2) as Date
				,count(distinct IdTransaction)as count_transaction
from db_hiring_test.raw_transactions_table
		where ImportDate= '2021-02-05 23:59:59' #Dados mais recentes 
		and Status = 'Confirmed' #Transações confirmadas
		and addressorigin != addressdestination #Transações com origem e destino diferentes
		and addressorigin != '' #Transações com origem 
		and  addressdestination !='' #Transações com destino
group by SUBSTRING(sentdate,9,2)
order by 2 desc;""",cnx)

question_2.head()


# In[36]:


#Questão 3
#resposta: O dia da semana com maior numero de transações é sexta feira, o que no mysql vem como o dia 4.

question_3 = pd.read_sql_query("""select distinct count(distinct idtransaction) as count_transaction
				,week_day,number_day_week
from    	
					(select distinct *,
					                case when number_day_week = 0 then 'segunda' 
					                     when number_day_week = 1 then 'terça'
										 when number_day_week = 2 then 'quarta'
										 when number_day_week= 3 then 'quinta'
										 when number_day_week = 4 then 'sexta'
										 when number_day_week = 5 then 'sabado'
										 when number_day_week = 6 then 'domingo'
														else null end as week_day
 from 
              (Select *
                     ,weekday(substring(Sentdate,1,10)) as number_day_week
               from db_hiring_test.raw_transactions_table as tra) as a) as b;""",cnx)

question_3.head()


# In[37]:


#Questão 4
#Resposta: As transações abaixo necessitam ser revistas com a equipe responsavel por estarem com o mesmo endereço de origem e destino.

question_4 = pd.read_sql_query("""select distinct idtransaction
				,addressorigin
                ,addressdestination 
from db_hiring_test.raw_transactions_table
		where addressorigin = addressdestination
		or(addressorigin = ''
		or addressdestination = '');
""",cnx)

question_4.head()


# In[38]:


#Questão 5
#Resposta: A carteira com maior receita é a-83.

question_5 = pd.read_sql_query("""Select distinct addressorigin
				,sum(totalsent) as total 
from db_hiring_test.raw_transactions_table
		where ImportDate= '2021-02-05 23:59:59' #dados mais recentes 
		and Status = 'Confirmed' #Transações confirmadas
		and addressorigin != addressdestination #Transações com origem e destino diferentes
		and addressorigin != '' #Transações com origem
		and  addressdestination !='' #Transações com destino
group by addressorigin
order by total desc;
""",cnx)

question_5.head()


# In[39]:


#-----------------------------------------------Etapa 2---------------------------------------------------------#
##Analise de base 

tabela_valid_2 = pd.read_sql_query("""select distinct 
	* 
from db_hiring_test.raw_transactions_fee;
""",cnx)

tabela_valid_2.head()


# In[40]:


#Unindo as tabelas através do Join das tabelas
Join = pd.read_sql_query("""select *,
	  ((`fee-percentage`)/100)*replace(totalsent,',','') as taxa
from db_hiring_test.raw_transactions_table as tra
		inner join db_hiring_test.raw_transactions_fee as taxa
			on cast((replace(tra.totalsent,',','')) as float) > cast(`range-start`as float)
			and cast((replace(tra.totalsent,',','')) as float) < cast(`range-end`as float)
			and ImportDate = '2021-02-05 23:59:59' #dados mais recentes 
			and Status = 'Confirmed' #Transações confirmadas 
""",cnx)

Join.head()


# In[46]:


#Criando a tabela temporária

tabela_temporaria=cursor.execute("""CREATE TEMPORARY TABLE db_hiring_test.TempJoin
                           select * 
                                  ,((`fee-percentage`)/100)*replace(totalsent,',','') as taxa
						   from db_hiring_test.raw_transactions_table as tra
									inner join db_hiring_test.raw_transactions_fee as taxa
										on cast((replace(tra.totalsent,',','')) as float) > cast(`range-start`as float)
										and cast((replace(tra.totalsent,',','')) as float) < cast(`range-end`as float)
										and ImportDate = '2021-02-05 23:59:59' #dados mais recentes 
										and Status = 'Confirmed' #Transações confirmadas 
											;
""")


# In[48]:


#Questão 1
#Resposta: A carteira que mais pagou taxa em janeiro de 2021 foi a A-99, e maior pagamento de percentual seria A-62.
##Calculo de maior carteira pagadora de percentual.

question_ETP2_1 = pd.read_sql_query("""select distinct addressorigin
				, sum(`fee-percentage`) as soma_taxa 
from db_hiring_test.TempJoin
	where sentdate like '2021-01%' #filtro para apenas janeiro de 2021
group by addressorigin
order by 2 desc;
""",cnx)

question_ETP2_1 .head()


# In[49]:


##Calculo de maior carteira pagadora de valor como taxa

question_ETP2_1_2 = pd.read_sql_query("""select distinct addressorigin
				,sum(taxa) as taxa 
from db_hiring_test.TempJoin
	where sentdate like '2021-01%' #filtro para apenas janeiro de 2021
group by addressorigin
order by 2 desc;
""",cnx)

question_ETP2_1_2.head()


# In[50]:


#Questão 2
#Resposta: A carteira que mais pagou percentual de taxa em fevereiro de 2021 foi a A-82, e a de valor bruto em taxa foi A-29.
##Calculo de maior carteira pagadora de percentual.

question_ETP2_2_1 = pd.read_sql_query("""select distinct addressorigin
				,sum(`fee-percentage`) as percentual 
from db_hiring_test.TempJoin
	where sentdate like '2021-02%' #filtro para apenas fevereiro de 2021
group by addressorigin
order by 2 desc;
""",cnx)

question_ETP2_2_1.head()


# In[51]:


##Calculo de maior carteira pagadora de valor de taxa

question_ETP2_2_2 = pd.read_sql_query("""select distinct addressorigin
				,sum(taxa) as taxa 
from db_hiring_test.TempJoin
	where sentdate like '2021-02%'#filtro para apenas fevereiro de 2021
group by addressorigin
order by 2 desc;
""",cnx)

question_ETP2_2_2.head()


# In[52]:


#Questão 3
#Resposta: A carteira que mais pagou percentual taxa  foi a A-82 e a que mais pagou em valor de taxa foi A-99
##Calculo de maior carteira pagadora de percentual.


question_ETP2_3_1 = pd.read_sql_query("""select distinct addressorigin
				,sum(`fee-percentage`) as percentual 
from db_hiring_test.TempJoin
group by addressorigin
order by 2 desc;
""",cnx)

question_ETP2_3_1.head()


# In[53]:


##Calculo de maior carteira pagadora de valor em taxa

question_ETP2_3_2 = pd.read_sql_query("""select distinct addressorigin
				,sum(taxa)as taxa
                ,sum(`fee-percentage`) as percentual 
from db_hiring_test.TempJoin
group by addressorigin
order by 2 desc;
""",cnx)

question_ETP2_3_2.head()


# In[54]:


#Questão 4 
# Resposta: A média de valor de taxa é R$2.1745,80 e o percentual médio é 5,88%

#Média de percentual e valor de taxa 
question_ETP2_4 = pd.read_sql_query("""select distinct round(avg(taxa),2)as taxa
				,round(avg(`fee-percentage`),2) as percentual
from db_hiring_test.TempJoin;""",cnx)

question_ETP2_4.head()


# In[ ]:




