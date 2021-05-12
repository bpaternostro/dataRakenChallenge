import saspy as sas

import constants as const


class Utils:

    def __init__(self,log):
        #starting sas session
        self.logging=log
        self.status_process=True
        self.sas = saspy.SASsession(cfgfile=const.SAS_CONFIG_FILE)        
        self.query_final_process='''select Rlob, Slob, Alob, MonedaId, TrnQtr, Sum(PremEarnUsGrsNet) as PrmErnUsGrsNet Format COMMA20.2,Sum(PremEarnUsGNCed) as PrmErnUsGNCed Format COMMA20.2
                                From wan.tblprmsum
                                group by Rlob, Slob, Alob, MonedaId, TrnQtr
                                HAVING TrnQtr= Datepart('31MAR2021:00:00:00'dt'''
        
        
    def get_status_process(self):
        return self.status_process

    def get_query_final_process(self):
        return self.query_final_process

    # tengo que ver como devuelvo una lista aca
    def execute_process(self, process, prompt_o):
        try:
            results_dict = sas.submit(process,
                prompt = prompt_o
                )
            sas_log = sas_output['LOG']
        except Exception as e:
            self.logging.error("There were problems execution SAS process: "+str(e))
            self.status_process=False

        return sas_log

    def execute_algoritmo01(self):
        # here we should have an method from SASpy or a WS to execute the SAS process
        try:
            pro='''%include "/opt/data/SAS_S/sas94/sasdata/exploration/LatAm/Mexico/SAS Base Files/02 Master Code/00 - modMst.sas";'''
        except Exception as e:
            self.logging.error("Problems executing method execute_algoritmo01: "+str(e))
            self.status_process=False
        
        return self.execute_process(pro,None)

    # Se crea un listado de todos los trimestres nivel Calendario (CalQtr), partiendo de las fechas históricas DateStr y terminando en DateEnd, declaradas en el modMst*/    
    def execute_algoritmo_05_a(self):
        if self.status_process:
            try:
                pro='''%Log('Inicia modPrmErn');
                data Calendar_Qtr_List;
                    format AcdQtr date9.;
                    AcdQtr = &gDateStr;
                        do while(AcdQtr <= &gDateEnd);
                        output ;
                        AcdQtr = intnx("quarter",AcdQtr,1,"end");
                        end;
                        run;'''
                self.listado_trimestres=self.execute_process(pro,None)
            except Exception as e:
                self.logging.error("Problems executing method execute_algoritmo_05_a: "+str(e))
                self.status_process=False
        return True
    
    # Seleccionamos los campos necesarios para el proceso del devengamiento    
    def execute_algoritmo_05_b(self):
        if self.status_process:
            try:
                pro='''SELECT 
                    PolizaId, EntidadId, EndosoId, Rlob, Slob, Alob, Plob, ClaveId, EstadoId, MonedaId, ClaseId, RiesgoCat, Riesgoid, PeriodoId,
                    datepart(FechaRegistro) as FechaRegistro format = date9., 
                    datepart(TrnQtr) as TrnQtr format = date9., 
                    datepart(PolQtr) as PolQtr format = date9., 
                    datepart(InicioVigenciaPoliza) as InicioVigenciaPoliza format = date9., 
                    datepart(FinVigenciaPoliza) as FinVigenciaPoliza format = date9., 
                    datepart(InicioVigencia) as InicioVigencia format = date9., 
                    datepart(FinVigencia) as FinVigencia format = date9., 
                    PrimaNetaMto, 
                    PrimaRetenidaMto, 
                    PrimaCedidaMto, 
                    ComisionMto, 
                    RecargoMto, 
                    DerechoMto, 
                    BonificacionPct, 
                    DescuentoPct, 
                    FactorMercado as MercadoRelCuo label='MercadoRelCuo' , 
                    TipoEndoso, 
                    MovimientoEndosoId AS MovEndId label = 'MovEndId'
                    FROM tblPrmKey
                    WHERE  calculated FinVigencia > mdy(12,31,99)
                    ORDER BY Rlob, Slob, Alob, Plob, EstadoId, MonedaId, ClaseId, RiesgoCat, Riesgoid, TrnQtr, PolQtr;
                    quit;'''
                self.tbl_prmern0=self.execute_process(pro,None)
            except Exception as e:
                self.logging.error("Problems executing method execute_algoritmo_05_b : "+str(e))
                self.status_process=False
        return True

    # Se asignan las variables para las validaciones de fecha, los signos dependiendo del tipo de endoso para los expuestos y se definen los tipos de Primas */
    def execute_algoritmo_05_c(self):
        if self.status_process:
            try:
                pro='''data tblPrmErn2;
                        set tblPrmErn0;'''
            except Exception as e:
                self.logging.error("Problems executing method execute_algoritmo_05_c : "+str(e))
                self.status_process=False                    
        return self.execute_process(pro,None)

    # 05_D subPrmErnDates */
    def execute_algoritmo_05_d(self):
        if self.status_process:
            try:
                pro='''format strDate tIniDate tFinDate tAcdStr tAcdEnd tAcdPri tAcdCur tAcdFst tCalEnd date9.;

                    strDate = intnx("month",&gDateStr,-1,"end"); 

                    tIniDate = InicioVigencia; /*exposure period StartDate and EndDate limited by policy start and end dates*/
                    tFinDate = FinVigencia;
                        If tIniDate < InicioVigenciaPoliza or tIniDate > FinVigenciaPoliza Then tIniDate = InicioVigenciaPoliza;
                        If tFinDate < InicioVigenciaPoliza or tFinDate > FinVigenciaPoliza Then tFinDate = FinVigenciaPoliza;
                        If tIniDate < strDate 
                        or tIniDate > &gDateEnd
                        or tFinDate < strDate 
                        or tFinDate > &gDateEnd 
                        Then do;
                            tIniDate = intnx("month",FechaRegistro,0,"end") -5;
                            tFinDate = tIniDate + 30;
                        end;
                        
                        If tIniDate = tFinDate Then tFinDate = tIniDate + 1;
                        If tIniDate < tFinDate Then do;
                            tAcdStr = tIniDate;
                            tAcdEnd = tFinDate;
                        end;
                            Else do;
                                /* We have dirty transactions with Ini>Fin*/
                                tAcdStr = tFinDate;
                                tAcdEnd = tIniDate;
                            end;

                        ActuDayu = tAcdEnd - tAcdStr;
                        tAcdPri = tAcdStr;
                        tAcdCur = intnx("quarter",tAcdStr,0,"end"); /*calc last day quarter*/
                        tAcdFst = tAcdCur;
                        DaysYear = 365;
                        *AcdQtr = intnx("quarter",tAcdCur,-1,"end");
                        tCalEnd = intnx("quarter",tAcdEnd,0,"end");'''
            except Exception as e:
                self.logging.error("Problems executing method execute_algoritmo_05_d : "+str(e))
                self.status_process=False                   
        return self.execute_process(pro,None)

    # 05_E subPrmErnPrem*/
    def execute_algoritmo_05_e(self):
        if self.status_process:
            try:
                pro='''format VehsSign best12.;
                    Select;
                            when (MovEndId = 2) VehsSign = 1;                   /*	Inclusion de Riesgo	Added Feb 02, 2005*/
                            when (MovEndId = 5) VehsSign = -1;					/*	Cancelacion de Riesgo Added Feb 02, 2005*/
                            when (MovEndId = 10) VehsSign = 1;					/*	Alta Inicial*/
                            when (MovEndId = 11) VehsSign = 1;					/*	Inciso Nuevo*/	
                            when (MovEndId = 12) VehsSign = 1;					/*	Does not exist*/
                            when (MovEndId = 13) VehsSign = -1;					/*	Cancelacion de Poliza*/
                            when (MovEndId = 14) VehsSign = -1;					/*	Cancelacion de Inciso*/
                            when (MovEndId = 15) VehsSign = -1;					/*	Cancelacion automatica*/
                            when (MovEndId = 16) VehsSign = 1;					/*	Rehabilitacion de Poliza*/
                            when (MovEndId = 17) VehsSign = 1;					/*	Rehabilitacion de Inciso*/
                            when (MovEndId = 69) VehsSign = 1;					/*	Endoso Incancelable*/
                            otherwise VehsSign = 0;
                    end;
                            If MovEndId in( 6, 36) Then do;
                                If TipoEndoso = "A " Then VehsSign = 1;
                                    Else  VehsSign = -1;
                            end;

                    PremWritNetNet = PrimaNetaMto;
                    PremWritGrsNet = PrimaNetaMto / ((1 - BonificacionPct));
                    PremWritGrsGrs = PrimaNetaMto / ((1 - BonificacionPct) * (1 - DescuentoPct));
                    PremWritGrsFac = PrimaNetaMto / (((1 - BonificacionPct) * (1 - DescuentoPct)) * MercadoRelCuo);
                    PremWritGNCed = PrimaCedidaMto;
                    If PremWritGNCed in(., 0) 
                        Then PremWritGNRet = PremWritGrsNet;
                            Else PremWritGNRet = PrimaRetenidaMto;
                    
                    
                    PremUeprUsNetNet = PremWritNetNet;
                    PremUeprUsGrsNet = PremWritGrsNet;
                    PremUeprUsGrsGrs = PremWritGrsGrs;
                    PremUeprUsGrsFac = PremWritGrsFac;
                    PremUeprUsGNRet = PremWritGNRet;
                    PremUeprUsGNCed = PremWritGNCed;
                    PremEarnUsNetNet = 0;
                    PremEarnUsGrsNet = 0;
                    PremEarnUsGrsGrs = 0;
                    PremEarnUsGrsFac = 0;
                    PremEarnUsGNRet = 0;
                    PremEarnUsGNCed = 0;
                    RecargoWrit = RecargoMto;
                    RecargoUepr = RecargoWrit;
                    RecargoEarn = 0;

                    If DerechoMto = . Then  DerechoWrit = 0;
                        Else  DerechoWrit = DerechoMto;
                    
                    DerechoUepr = DerechoWrit;
                    DerechoEarn = 0;
                    id = cats(tAcdStr,tCalEnd);
                    run;'''

            except Exception as e:
                self.logging.error("Problems executing method execute_algoritmo_05_e : "+str(e))
                self.status_process=False                   
        return self.execute_process(pro,None)

    # 05_F Se crea con un JOIN una lista de las distintas combinaciones de AcdStr y CalEnd */
    def execute_algoritmo_05_f_0(self):
        if self.status_process:
            try:
                pro='''create table PrmErnDateRanges as
                    select distinct tAcdStr, tCalEnd, cats(tAcdStr,tCalEnd) as id
                    from tblPrmErn2
                    order by id;                                
                    quit;
                    '''
            except Exception as e:
                    self.logging.error("Problems executing method execute_algoritmo_05_f_0 : "+str(e))
                    self.status_process=False                   
        return self.execute_process(pro,None)  

    def execute_algoritmo_05_f_1(self):
        if self.status_process:
            try:
                pro='''select A.*, B.*
                        from PrmErnDateRanges as A left join Calendar_Qtr_List as B
                        ON A.tAcdStr <= B.AcdQtr <= A.tCalEnd   
                        order by id;
                        quit;
                    '''
            except Exception as e:
                    self.logging.error("Problems executing method execute_algoritmo_05_f_2 : "+str(e))
                    self.status_process=False                   
        return self.execute_process(pro,None)        

    #05_G Se hace JOIN para crear tabla tblPrmErn3 por cada uno de los periodos usando la tabla DateRangesAll */
    def execute_algoritmo_05_g(self):
        if self.status_process:
            try:
                pro='''select A.*, B.AcdQtr
                from tblPrmErn2 as A left join daterangesAll as B
                on A.id = B.id;
                quit;'''
            except Exception as e:
                    self.logging.error("Problems executing method execute_algoritmo_05_g : "+str(e))
                    self.status_process=False                   
        return self.execute_process(pro,None)        

    #05_H Se devenga la prima usando las fechas y variables que se definieron anteriormente */
    def execute_algoritmo_05_h_i(self):
        if self.status_process:
            try:
                pro='''data tblPrmErn4;
                set tblPrmErn3 (drop = id);
                    qtr_boundaries = INTCK('quarter',tAcdStr,AcdQtr);
                        if qtr_boundaries > 0 then tAcdPri = intnx("quarter",AcdQtr,-1,"end");

                    tAcdCur = min(AcdQtr,tAcdEnd);
                    total_period = (tAcdEnd - tAcdStr); /*since my method earns from the full written amount for each qtr, use the total time period rather than remaining unearned days, otherwise I will get cumulative results*/
                    ActuDayu = tAcdEnd - tAcdPri;

                    if qtr_boundaries=0 				/* Cambie esta parte para agregar medio dia al inicio de la vigencia */ 
                        then ActuDaye = tAcdCur - tAcdPri + 0.5;
                    else
                        ActuDaye = tAcdCur - tAcdPri;
                    If AcdQtr < tAcdEnd Then do;
                    PremEarnUsNetNet = PremWritNetNet * ActuDaye / total_period;
                    PremEarnUsGrsNet = PremWritGrsNet * ActuDaye / total_period;
                    PremEarnUsGrsGrs = PremWritGrsGrs * ActuDaye / total_period;
                    PremEarnUsGrsFac = PremWritGrsFac * ActuDaye / total_period;
                    PremEarnUsGNRet = PremWritGNRet * ActuDaye / total_period;
                    PremEarnUsGNCed = PremWritGNCed * ActuDaye / total_period;
                    RecargoEarn = RecargoWrit * ActuDaye / total_period;
                    DerechoEarn = DerechoWrit * ActuDaye / total_period;
                    /*if VehsSign = -1 then*/ VehsEarn = VehsSign * ActuDaye / DaysYear; 
                    /*else VehsEarn = ActuDaye / DaysYear;*/
                    VehsInfr = VehsSign;
                end;
                    
                If qtr_boundaries = 0 then
                    do;
                        PremUeprUsNetNet = (PremWritNetNet*(ActuDayu/total_period))- PremEarnUsNetNet ;
                        PremUeprUsGrsNet = (PremWritGrsNet *(ActuDayu/total_period))- PremEarnUsGrsNet ;
                        PremUeprUsGrsGrs = (PremWritGrsGrs *(ActuDayu/total_period))- PremEarnUsGrsGrs ;
                        PremUeprUsGrsFac = (PremWritGrsFac *(ActuDayu/total_period))- PremEarnUsGrsFac ;
                        PremUeprUsGNRet = (PremWritGNRet *(ActuDayu/total_period))- PremEarnUsGNRet ;
                        PremUeprUsGNCed = (PremWritGNCed *(ActuDayu/total_period))- PremEarnUsGNCed ;
                        RecargoUepr = (RecargoWrit *(ActuDayu/total_period)) - RecargoEarn;
                        DerechoUepr = (DerechoWrit *(ActuDayu/total_period)) - DerechoEarn;
                    end;
                Else
                    do;
                        PremUeprUsNetNet = (PremWritNetNet*((ActuDayu-0.5)/total_period))- PremEarnUsNetNet ;
                        PremUeprUsGrsNet = (PremWritGrsNet *((ActuDayu-0.5)/total_period))- PremEarnUsGrsNet ;
                        PremUeprUsGrsGrs = (PremWritGrsGrs *((ActuDayu-0.5)/total_period))- PremEarnUsGrsGrs ;
                        PremUeprUsGrsFac = (PremWritGrsFac *((ActuDayu-0.5)/total_period))- PremEarnUsGrsFac ;
                        PremUeprUsGNRet = (PremWritGNRet *((ActuDayu-0.5)/total_period))- PremEarnUsGNRet ;
                        PremUeprUsGNCed = (PremWritGNCed *((ActuDayu-0.5)/total_period))- PremEarnUsGNCed ;
                        RecargoUepr = (RecargoWrit *((ActuDayu-0.5)/total_period)) - RecargoEarn;
                        DerechoUepr = (DerechoWrit *((ActuDayu-0.5)/total_period)) - DerechoEarn;
                    end;
                    
                If AcdQtr >= tAcdEnd Then do;
                    PremEarnUsNetNet = PremUeprUsNetNet;
                    PremEarnUsGrsNet = PremUeprUsGrsNet;
                    PremEarnUsGrsGrs = PremUeprUsGrsGrs;
                    PremEarnUsGrsFac = PremUeprUsGrsFac;
                    PremEarnUsGNRet = PremUeprUsGNRet;
                    PremEarnUsGNCed = PremUeprUsGNCed;
                    RecargoEarn = RecargoUepr;
                    DerechoEarn = DerechoUepr;
                    /*if VehsSign = -1 then*/ VehsEarn = VehsSign * (ActuDayu-0.5) / DaysYear; 
                    /*else VehsEarn = ActuDayu / DaysYear;*/
                    VehsInfr = 0;
                End;

                If qtr_boundaries = 0 then
                    do;
                        PremUeprUsNetNet = (PremWritNetNet*(ActuDayu/total_period))- PremEarnUsNetNet ;
                        PremUeprUsGrsNet = (PremWritGrsNet *(ActuDayu/total_period))- PremEarnUsGrsNet ;
                        PremUeprUsGrsGrs = (PremWritGrsGrs *(ActuDayu/total_period))- PremEarnUsGrsGrs ;
                        PremUeprUsGrsFac = (PremWritGrsFac *(ActuDayu/total_period))- PremEarnUsGrsFac ;
                        PremUeprUsGNRet = (PremWritGNRet *(ActuDayu/total_period))- PremEarnUsGNRet ;
                        PremUeprUsGNCed = (PremWritGNCed *(ActuDayu/total_period))- PremEarnUsGNCed ;
                        RecargoUepr = (RecargoWrit *(ActuDayu/total_period)) - RecargoEarn;
                        DerechoUepr = (DerechoWrit *(ActuDayu/total_period)) - DerechoEarn;
                    end;
                Else
                    do;
                        PremUeprUsNetNet = (PremWritNetNet*((ActuDayu-0.5)/total_period))- PremEarnUsNetNet ;
                        PremUeprUsGrsNet = (PremWritGrsNet *((ActuDayu-0.5)/total_period))- PremEarnUsGrsNet ;
                        PremUeprUsGrsGrs = (PremWritGrsGrs *((ActuDayu-0.5)/total_period))- PremEarnUsGrsGrs ;
                        PremUeprUsGrsFac = (PremWritGrsFac *((ActuDayu-0.5)/total_period))- PremEarnUsGrsFac ;
                        PremUeprUsGNRet = (PremWritGNRet *((ActuDayu-0.5)/total_period))- PremEarnUsGNRet ;
                        PremUeprUsGNCed = (PremWritGNCed *((ActuDayu-0.5)/total_period))- PremEarnUsGNCed ;
                        RecargoUepr = (RecargoWrit *((ActuDayu-0.5)/total_period)) - RecargoEarn;
                        DerechoUepr = (DerechoWrit *((ActuDayu-0.5)/total_period)) - DerechoEarn;
                    end;

                if Riesgoid in (84,143,146,148,152,145,147,149,150,151) then VehsEarn = 0;
                run;            
                Proc delete data = tblPrmErn2; Run;
                Proc delete data = tblPrmErn3; Run;'''
            
            except Exception as e:
                    self.logging.error("Problems executing method execute_algoritmo_05_h_i : "+str(e))
                    self.status_process=False                   
        return self.execute_process(pro,None)
        
    # 05_J Se crea tabla tblPrmErn a partir de la tabla tblPrmErn4 con los campos necesarios para nuestros análisis */
    def execute_algoritmo_05_j(self): 
        if self.status_process:
            try:
                pro='''data tblPrmErn;
                set tblPrmErn4(keep=Polizaid Entidadid Endosoid Claveid Rlob slob alob Plob estadoid monedaid claseid riesgocat PolQtr AcdQtr TrnQtr PrEmEarnUsNetNet PremEarnUsGrsNet 	
                PremEarnUsGrsGrs PremEarnUsGrsFac PremEarnUsGNRet PremEarnUsGNCed DerechoEarn RecargoEarn VehsEarn VehsInfr);
                run;
                /*Proc delete data = tblPrmErn4; Run;*/
                %Log('Termina modPrmErn');'''
            except Exception as e:
                self.logging.error("Problems executing method execute_algoritmo_05_j : "+str(e))
                self.status_process=False                   
        return self.execute_process(pro,None) 
        

    #06_A Se crea la tabla tblPrmSum a partir de la tblPrmErn, con la informacion al nivel que se necesita para construir los triangulos posteriormente */
    def execute_algoritmo_06_a(self): 
        if self.status_process:
            try:
                pro='''%Log('Inicia modPrmSum');
                %procmeans(tblPrmErn,	
                Wan.tblPrmSum,	
                Rlob Slob Alob Plob EstadoId ClaseId MonedaId RiesgoCat TrnQtr PolQtr AcdQtr,	
                PremEarnUsNetNet PremEarnUsGrsNet PremEarnUsGrsGrs PremEarnUsGrsFac PremEarnUsGNRet PremEarnUsGNCed DerechoEarn RecargoEarn VehsEarn VehsInfr);	

                %Log('Termina modPrmSum');
                %Log('Termina Proceso');

                Data wan.logproceso;
                set LogProceso;
                run;'''
            except Exception as e:
                self.logging.error("Problems executing method execute_algoritmo_06_a : "+str(e))
                self.status_process=False                   
        return self.execute_process(pro,None)

    #06_B Se convierte a real la tabla tblPrmErn */
    def execute_algoritmo_06_b(self):
        if self.status_process:  
            try:
                pro='''Data wan.tblPrmErn;
                set tblPrmErn;
                run;'''
            except Exception as e:
                self.logging.error("Problems executing method execute_algoritmo_06_b : "+str(e))
                self.status_process=False                   
        return self.execute_process(pro,None)        

    def get_table_data(self,table_name,export_type):        
        try:
            data=sas.sasdata(table=table_name, libref="SASHELP",results=export_type)
        except Exception as e:
            self.logging.error("There were problems execution the call to the table: "+str(e))
            self.status_process=False   
        return data


    def get_rdd(self,sp,table_name,from):
        # Creates a spark data frame from pandas and then create the rdd
        try:
            self.logging.info("Creating RDD with table tblPrmErn3")            
            aux_rdd=sp.createDataFrame(self.get_table_data(table=table_name,export_type=from)) 
        except Exception as e:
            self.logging.error("There were problems execution with get_rdd: "+str(e))
            self.status_process=False    
        
        return aux_rdd.rdd

    '''
    def execute_all_process(self):
        result_list=[]        
        result_list.append(self.execute_algoritmo01())
        result_list.append(self.execute_algoritmo_05_a())
        result_list.append(self.execute_algoritmo_05_b())
        result_list.append(self.execute_algoritmo_05_c())
        result_list.append(self.execute_algoritmo_05_d())
        result_list.append(self.execute_algoritmo_05_e())
        result_list.append(self.execute_algoritmo_05_f())
        result_list.append(self.execute_algoritmo_05_g())
        result_list.append(self.execute_algoritmo_05_h_i())
        result_list.append(self.execute_algoritmo_05_h_j())
        result_list.append(self.execute_algoritmo_06_a())
        result_list.append(self.execute_algoritmo_06_b())        
        return not (False in result_list) '''



