#!/usr/bin/env python
# coding: utf-8

from yargy import rule, and_, or_, Parser, not_
from yargy.predicates import gram, normalized, caseless, gte, lte, in_
from yargy.pipelines import morph_pipeline
from datetime import datetime
from yargy.tokenizer import MorphTokenizer
import time
import re
import pymorphy2
morph = pymorphy2.MorphAnalyzer()


def extract(text):

    f = open('deseases')
    deseases = f.read()
    deseases = deseases.split('\n')

    text = text.replace('\ufeff', '')
    text = text.replace('\n', ' \n ')
    text = text.replace('\\', ' ')

    symptoms = ['Дата рождения', 'Дата осмотра','Дата заболевания', 'Возраст', 'Болен дней','Болен часов','Возраст в днях','Время поступления', 
                'Время заболевания', 'рост','вес', 'IMT', 'давление диаст', 'давление сист', 'температура поступления','мах температура', 'Т-Ан01', 'Т-Ан03', 
                'пол', 'др заболевания в анамнезе', 'кем направлен', 'побочное действие лекартсв','аллергическая реакция', 'озноб', 'слабость', 'вялость','головная боль', 
                'нарушение сна', 'нарушение аппетита', 'ломота','тошнота', 'нарушение сознания', 'Судороги', 'Парестезии', 'эритема', 
                'с четкими границами', 'валик', 'боль','Гиперемия', 'Отек', 'Лимфаденит', 'Лимфангит', 'квартира, дом','контакт с зараженными','речная рыба','провоцирущие факторы',
                'предрасполагающие факторы','кол-во сопут заболеваний','соц категория','сопутствующий диагноз','основной диагноз', 'контакт с зараженными', 'пищевой анамнез',
                'раневые ворота', 'аллергия на лекарства', 'клещ', 'географический анамнез', 'вредные привычки', 'домашние животные', 'условия труда','избыточное питание',
                'ППТ', 'ЛПТ', 'бытовые условия', 'питание', 'интоксикация', 'ЧСС', 'болезненность лимфоузлов', 'увеличенность лимфоузлов','размер лимфоузлов', 'острое начало']

    dict_symp = dict.fromkeys(symptoms)
    dict_index = dict.fromkeys(symptoms)

    dates_lst = []
    dates_spans = []

    # Rule for dates detecting
    DAY = and_(gte(1),lte(31))
    MONTH = and_(gte(1),lte(12))
    YEAR = and_(gte(1),lte(19))
    YEARFULL = and_(gte(1900),lte(2020))
    DATE = or_(
        rule(YEAR,'.',MONTH,'.',DAY),
        rule(DAY,'.',MONTH,'.',YEAR),
        rule(DAY,'.',MONTH,'.',YEARFULL),
        rule(DAY,'.',MONTH),
        rule(DAY,'.',MONTH,YEARFULL),
        rule(DAY,'.',MONTH,YEAR))

    parser = Parser(DATE)
    for match in parser.findall(text):
        dates_lst.append(''.join([_.value for _ in match.tokens]))
        dates_spans.append(match.span)

    # Sometimes we dont have information about birthday and we should check difference between years
    # in first two dates to determine there is information about birthday or not
    if int(dates_lst[1][-2:])-int(dates_lst[0][-2:])<0:
        # According medical cards dates have this order
        dict_symp['Дата рождения'] = dates_lst[0]
        dict_symp['Дата осмотра'] = dates_lst[1]
        dict_symp['Дата заболевания'] = dates_lst[2]
        dict_index['Дата рождения'] = dates_spans[0]
        dict_index['Дата осмотра'] = dates_spans[1]
        dict_index['Дата заболевания'] = dates_spans[2]
    else: 
        birth = None
        dict_symp['Дата осмотра'] = dates_lst[0]
        dict_symp['Дата заболевания'] = dates_lst[1]
        dict_index['Дата осмотра'] = dates_spans[0]
        dict_index['Дата заболевания'] = dates_spans[1]

    # If date was written without year, we take year from previous date
    if len(dict_symp['Дата заболевания'])==5:
        dict_symp['Дата заболевания'] += dict_symp['Дата осмотра'][dict_symp['Дата осмотра'].rfind('.'):]

    # Rule for detecring dates with such situation "болен 5 дней"
    DAY_RULE = morph_pipeline(['дней'])
    parser = Parser(DAY_RULE)
    day_lst = []
    for match in parser.findall(text):
        day_lst.append((match.span, [_.value for _ in match.tokens]))

    if day_lst and dict_symp['Дата заболевания'] is None:
        dict_symp['Дата заболевания'] = text[day_lst[0][0][0]-20:day_lst[0][0][0]+20]
        dict_symp['Дата заболевания'] = re.findall(r'\d+', dict_symp['Дата заболевания'])[0]
        dict_symp['Дата заболевания'] = str(int(dict_symp['Дата осмотра'][:2])-int(dict_symp['Дата заболевания']))
        dict_symp['Дата заболевания'] = dict_symp['Дата заболевания']+dict_symp['Дата осмотра'][2:]
        dict_index['Дата заболевания'] = day_lst[0][0]

    # Rule for detecting Age
    age_lst = []
    age_spans = []

    AGE = and_(gte(0),lte(100))
    AGE_RULE = or_(rule("(",AGE,")"),
                  rule(gram('ADJF'),",",AGE))

    parser = Parser(AGE_RULE)
    for match in parser.findall(text):
        s = ''.join([_.value for _ in match.tokens])
        age_lst.append((re.findall(r'\d+', s)[0]))
        age_spans.append(match.span)

    if age_lst:
        dict_symp['Возраст'] = int(age_lst[-1])
        dict_index['Возраст'] = age_spans[-1]
    
    # Transform dates to datetime format to make calculations
    try:
        d1 = datetime.strptime(dict_symp['Дата осмотра'], '%d.%m.%Y')
    except:
        d1 = datetime.strptime(dict_symp['Дата осмотра'], '%d.%m.%y')
        d1 = d1.strftime('%d.%m.%Y')
        d1 = datetime.strptime(d1, '%d.%m.%Y')
    try:
        d2 = datetime.strptime(dict_symp['Дата заболевания'], '%d.%m.%Y')
    except:
        d2 = datetime.strptime(dict_symp['Дата заболевания'], '%d.%m.%y')
        d2 = d2.strftime('%d.%m.%Y')
        d2 = datetime.strptime(d2, '%d.%m.%Y')

    dict_symp['Болен дней'] = (d1 - d2).days
    dict_symp['Болен часов'] = (int(dict_symp['Болен дней'])-1)*24

    if dict_symp['Дата рождения'] is None:
        dict_symp['Возраст в днях'] = int(dict_symp['Возраст'])*365
    else:
        d1 = datetime.strptime(dict_symp['Дата осмотра'], '%d.%m.%Y')
        d2 = datetime.strptime(dict_symp['Дата рождения'], '%d.%m.%Y')
        dict_symp['Возраст в днях'] = (d1 - d2).days

    # Rule for time detecting
    time_lst = []
    time_spans = []

    HOURS = and_(gte(0),lte(24))

    MINUTES = and_(gte(0),lte(59))

    TIME = or_(rule(HOURS,':',MINUTES),
               rule(HOURS, normalized('час')),)

    parser = Parser(TIME)
    for match in parser.findall(text):
        s = (''.join([_.value for _ in match.tokens]))
        time_spans.append(match.span)
        s = s.replace('часов', ':00')
        s = s.replace('час', ':00')
        time_lst.append(s)

    # if we have only 1 date 'Время поступления' = 'Время заболевания'
    if time_lst: 
        dict_symp['Время поступления'] = time_lst[0]
        dict_symp['Время заболевания'] = time_lst[0]
        dict_index['Время поступления'] = time_spans[0]
        dict_index['Время заболевания'] = time_spans[0]
    if len(time_lst)>1: 
        dict_symp['Время заболевания'] = time_lst[1]
        dict_index['Время заболевания'] = time_spans[1]

    t1 = dict_symp['Время поступления']
    t2 = dict_symp['Время заболевания']
    delta = int(t1[:t1.find(':')])+24-int(t2[:t2.find(':')])
    dict_symp['Болен часов'] = dict_symp['Болен часов'] + delta

    # Rules for detecting Weight, Height and IMT
    HEIGHT = and_(gte(50),lte(250))
    WEIGHT = and_(gte(10),lte(150))

    HEIGHT_RULE = or_(rule(normalized('рост'),'-',HEIGHT),
                      rule(normalized('рост'),'–',HEIGHT),
                      rule(normalized('рост'),':',HEIGHT),
                      rule(normalized('рост'),HEIGHT))

    WEIGHT_RULE = or_(rule(normalized('вес'),'-',WEIGHT),
                      rule(normalized('вес'),'–',WEIGHT),
                      rule(normalized('вес'),':',WEIGHT),
                      rule(normalized('вес'),WEIGHT))

    height = None
    parser = Parser(HEIGHT_RULE)
    for match in parser.findall(text):
        height = (''.join([_.value for _ in match.tokens]))
        height_spans = match.span
        height = re.findall(r'\d+', height)[0]

    if height:
        dict_symp['рост'] = int(height)
        dict_index['рост'] = height_spans

    weight = None
    parser = Parser(WEIGHT_RULE)
    for match in parser.findall(text):
        weight = (''.join([_.value for _ in match.tokens]))
        weight = re.findall(r'\d+', weight)[0]
        weight_spans = match.span

    if weight:
        dict_symp['вес'] = int(weight)
        dict_index['вес'] = weight_spans

    if (dict_symp['рост'] is not None) and (dict_symp['вес'] is not None):
        dict_symp['IMT'] = round(dict_symp['вес']/(dict_symp['рост']/100*dict_symp['рост']/100),2)

    # Rules for detecting pressure
    ADSIST = and_(gte(50),lte(250))
    ADDIAST = and_(gte(20),lte(200))

    PRES_RULE = or_(rule('АД', ADSIST,'/',ADDIAST),
                    rule('АД', ADSIST,ADDIAST),
                    rule('АД', ADSIST, ':',ADDIAST),
                    rule('АД','-', ADSIST, '/',ADDIAST),
                    rule('А/Д', ADSIST, '/',ADDIAST),
                    rule('А/Д', ADSIST, ADDIAST),
                    rule('А/Д',' ', ADSIST, '/',ADDIAST),
                    rule(ADSIST, '/',ADDIAST))

    pres = None
    parser = Parser(PRES_RULE)
    for match in parser.findall(text):
        pres = (''.join([_.value for _ in match.tokens]))
        pres = re.findall(r'\d+', pres)
        pres_spans = match.span

    if pres:
        dict_symp['давление сист'] = int(pres[0])
        dict_symp['давление диаст'] = int(pres[1])
        dict_index['давление сист'] = pres_spans
        dict_index['давление диаст'] = pres_spans

    # Rule for detecting Pulse
    PULSE = and_(gte(40),lte(150))

    PULSE_RULE = or_(rule('ЧСС','-',PULSE),
                     rule('ЧСС',PULSE),
                     rule('ЧСС','-',PULSE),
                     rule('ЧСС','/',PULSE),
                     rule('пульс',PULSE),)

    pulse = None
    parser = Parser(PULSE_RULE)
    for match in parser.findall(text):
        pulse = (''.join([_.value for _ in match.tokens]))
        pulse = re.findall(r'\d+', pulse)
        pulse_spans = match.span

    if pulse:
        dict_symp['ЧСС'] = int(pulse[0])
        dict_index['ЧСС'] = pulse_spans

    #Rules for detecting temperatures
    DEGREES = and_(gte(34),lte(42))
    SUBDEGREES = and_(gte(0),lte(9))

    TEMP_RULE = or_(rule(DEGREES,',',SUBDEGREES),
                    rule(DEGREES,'.',SUBDEGREES),
                    rule(DEGREES))
    
    # Find 'Объективный статус', because this pert contains information about 'температура поступления'
    status = text[text.find('Объективный статус'): 
                  text.find('Объективный статус')+text[text.find('Объективный статус')+1:].find(' \n  \n')]
    temp_lst = []
    temp_spans = []
    parser = Parser(TEMP_RULE)
    for match in parser.findall(status):
        temp_lst.append(''.join([_.value for _ in match.tokens]))
        temp_spans.append(match.span)

    if temp_lst:
        dict_symp['температура поступления'] = temp_lst[0]
        dict_index['температура поступления'] = temp_spans[0]

    # Find temperatures in whole text
    temp_text = text[text.find('Жалобы'):]
    temp_lst = []
    temp_spans = []
    parser = Parser(TEMP_RULE)
    for match in parser.findall(temp_text):
        temp_lst.append(''.join([_.value for _ in match.tokens]))
        temp_spans.append(match.span)

    if temp_lst:
        if dict_symp['температура поступления'] is None:
            dict_symp['температура поступления'] = temp_lst[0]
            dict_index['температура поступления'] = temp_spans[0]
        dict_symp['мах температура'] = max([float(i.replace(',','.')) for i in temp_lst])

    if dict_symp['мах температура']>=38:
        dict_symp['Т-Ан01'] = 1
    else: 
        dict_symp['Т-Ан01'] = 0

    if dict_symp['мах температура']>=40:
        dict_symp['Т-Ан03'] = 3
    elif dict_symp['мах температура']>=39: 
        dict_symp['Т-Ан03'] = 2
    elif dict_symp['мах температура']>=38: 
        dict_symp['Т-Ан03'] = 1
    else:
        dict_symp['Т-Ан03'] = 0

    # Rule for detecting Sex
    sex_lst = []
    sex_spans = []
    SEX_RULE = or_(rule(normalized('женский')),
                     rule(normalized('мужской')))

    parser = Parser(SEX_RULE)
    for match in parser.findall(text):
        sex_lst.append(''.join([_.value for _ in match.tokens]))
        sex_spans.append(match.span)

    if sex_lst:
        dict_symp['пол'] = sex_lst[0]
        dict_index['пол'] = sex_spans[0]
        dict_symp['пол'] = dict_symp['пол'].lower().replace('женский', '2')
        dict_symp['пол'] = dict_symp['пол'].lower().replace('мужской', '1')
        dict_symp['пол'] = int(dict_symp['пол'])

    # Rule for detecting DISEASES
    DISEASES_RULE = morph_pipeline(deseases[:-1])

    # anamnez contains information about diseases of patient, but family anamnez contains 
    # information about diseases of patient, and we should remove this part
    anamnez = text[text.find('Анамнез'): text.find('Анамнез')+text[text.find('Анамнез')+1:].rfind('Анамнез')]
    family = anamnez[anamnez.find('Семейный'):anamnez.find('Семейный')+60]
    if family:
        anamnez = anamnez.replace(family,' ')
    anamnez = anamnez[:anamnez.rfind('Диагноз')]
    dis_lst = []
    dis_spans = []
    parser = Parser(DISEASES_RULE)
    for match in parser.findall(anamnez):
        dis_lst.append(' '.join([_.value for _ in match.tokens]))
        dis_spans.append(match.span)

    # Special rule for описторхоз
    OP_RULE = or_(rule(normalized('описторхоз'), not_(normalized('не'))))
    parser = Parser(OP_RULE)
    op_lst = []
    for match in parser.findall(anamnez):#text
        op_lst.append((match.span, [_.value for _ in match.tokens]))
    if op_lst:
        dis_lst.append(' описторхоз')
        dis_spans.append(match.span)

    # Special rule for туберкулез
    TUB_RULE = rule(normalized('туберкулез'), not_(normalized('отрицает')))
    parser = Parser(TUB_RULE)
    tub_lst = []
    for match in parser.findall(anamnez):#text
        tub_lst.append((match.span, [_.value for _ in match.tokens]))
    if tub_lst:
        dis_lst.append(' туберкулез')
        dis_spans.append(match.span)

    # Special rule for ВИЧ
    VICH_RULE = morph_pipeline(['ВИЧ'])
    parser = Parser(VICH_RULE)
    vich_lst = []
    for match in parser.findall(anamnez):#text
        vich_lst.append((match.span, [_.value for _ in match.tokens]))
    if vich_lst:
        text_vich = anamnez[match.span[1]-30:match.span[1]+30]
        TYPE = morph_pipeline(['отрицает'])
        parser = Parser(TYPE)
        vich_lst = []
        for match in parser.findall(text_vich):
            vich_lst.append((match.span, [_.value for _ in match.tokens]))
        if not vich_lst:
            dis_lst.append(' ВИЧ')
            dis_spans.append(match.span)
    
    if dis_lst:
        dis_lst = list(set(dis_lst))
        dict_symp['др заболевания в анамнезе'] = ', '.join(dis_lst)
        dict_index['др заболевания в анамнезе'] = dis_spans
        dict_symp['др заболевания в анамнезе'] = morph.parse(dict_symp['др заболевания в анамнезе'])[0].normal_form
            
    # Rules for detecting information about л/у
    LU_RULE = morph_pipeline(['лимфатические узлы', "лимфоузлы", "лу", "л/у"])
    parser = Parser(LU_RULE)
    lu_lst = []
    lu_spans = []
    for match in parser.findall(text):
        lu_lst.append((match.span, [_.value for _ in match.tokens]))
    if lu_lst:
        dict_symp['Лимфаденит'] = 0
        dict_index['Лимфаденит'] = lu_spans
        text_lu = text[match.span[1]-70:match.span[1]+70]
        TYPE = morph_pipeline(["болезненны", "болезненные", "болезнены"])
        parser = Parser(TYPE)
        lu_lst = []
        for match in parser.findall(text_lu):
            lu_lst.append((match.span, [_.value for _ in match.tokens]))
        if lu_lst:
            dict_symp['болезненность лимфоузлов'] = 1
            dict_index['болезненность лимфоузлов'] = match.span
            dict_symp['Лимфаденит'] = 1
        else:
            dict_symp['болезненность лимфоузлов'] = 0
            
        TYPE = morph_pipeline(['Увеличены', 'увеличенные'])
        parser = Parser(TYPE)
        lu_lst = []
        for match in parser.findall(text_lu):
            lu_lst.append((match.span, [_.value for _ in match.tokens]))
        if lu_lst:
            dict_symp['увеличенность лимфоузлов'] = 1
            dict_index['увеличенность лимфоузлов'] = match.span
            dict_symp['Лимфаденит'] = 1
        else:
            dict_symp['увеличенность лимфоузлов'] = 0
        
        number = and_(gte(0),lte(9))
        
        LU_SIZE_RULE = or_(rule(number,'.',number),
               rule(number,',',number))
        
        lu_lst = []
        lu_spans = []
        parser = Parser(LU_SIZE_RULE)
        for match in parser.findall(text_lu):
            lu_lst.append(''.join([_.value for _ in match.tokens]))
            lu_spans.append(match.span)
        if lu_lst:
            dict_symp['размер лимфоузлов'] = lu_lst[0]
            dict_index['размер лимфоузлов'] = lu_spans[0]

    # Rule for 'кем направлен'
    NAPR_RULE = morph_pipeline(['Поликлиника',"скорая помощь", "ск/помощь", 'СМП', "обратился"])

    napr = None
    napr_lst = []
    napr_spans = []
    parser = Parser(NAPR_RULE)
    for match in parser.findall(text):
        napr_lst.append(' '.join([_.value for _ in match.tokens]))
        napr_spans.append(match.span)
    if napr_lst:
        dict_index['кем направлен'] = napr_spans[0]
        napr = napr_lst[-1]
        napr = morph.parse(napr)[0].normal_form
    if napr == "обратиться":
        dict_symp['кем направлен'] = 3
    elif napr == "скорая помощь" or napr == "ск/помощь" or napr == 'смп'or napr == "ск / помощь" or napr == "скорой помощь" or napr == "скорую помощь":
        dict_symp['кем направлен'] = 1
    elif napr == "поликлиника":
        dict_symp['кем направлен'] = 2
        
    # Rule for allergy
    ALLERG_RULE = or_(rule(normalized('Аллергическая'),normalized('реакция'), normalized('на')),
                      rule(normalized('не'),normalized('переносит')))

    all_lst = []
    parser = Parser(ALLERG_RULE)
    for match in parser.findall(text):
        all_lst.append((match.span, [_.value for _ in match.tokens]))
    if all_lst:
        index = all_lst[0][0][1]
        dict_symp['аллергическая реакция'] = text[index:text[index:].find('.')+index]
        dict_index['аллергическая реакция'] = [all_lst[0][0][0], text[index:].find('.')+index]

    # Rules for different symptoms
    symptoms = [['озноб', 'познабливание'], 'слабость', ['вялость', 'разбитость'],'головная боль', 'нарушение сна', 
                'нарушение аппетита', 'ломота','тошнота', 'нарушение сознания','Судороги', 'Парестезии', ['эритема', 
                'эритематозная', 'эритематозно'], ['с четкими границами', 'границами четкими', 'четкими неровными краями',
                'с четкими краями', 'краями четкими' , 'четкими неровными краями', 'четкими контурами', 'языков пламени'], 
                ['валик', 'вал'], 'боль',['Гиперемия', 'гиперемирована'], 'Отек', 'Лимфангит', ['рана', "раневые ворота", 
                "входные ворота"],['клещ', "присасывание"], 'интоксикация', 'острое начало']
                
    for i in symptoms:
        sym_lst = []
        sym_spans = []
        if isinstance(i, str):
            SYM_RULE = morph_pipeline([i])
            parser = Parser(SYM_RULE)
            for match in parser.findall(text):
                sym_lst.append(' '.join([_.value for _ in match.tokens]))
                sym_spans.append(match.span)
            if sym_lst:
                dict_symp[i] = 1
                dict_index[i] = sym_spans[0]
            else:
                dict_symp[i] = 0
        else:
            SYM_RULE = morph_pipeline(i)
            parser = Parser(SYM_RULE)
            for match in parser.findall(text):
                sym_lst.append(' '.join([_.value for _ in match.tokens]))
                sym_spans.append(match.span)
            if sym_lst:
                dict_symp[i[0]] = 1
                dict_index[i[0]] = sym_spans[0]
            else:
                dict_symp[i[0]] = 0

    #This fuction used for features which have the same rule
    def find_feature(feature, RULE, RULE2, space=[40,40]):
        parser = Parser(RULE)
        lst = []
        for match in parser.findall(text):
            lst.append((match.span, [_.value for _ in match.tokens]))
        if lst:
            dict_index[feature] = match.span
            add_text = text[match.span[1]-space[0]:match.span[1]+space[1]]
            parser = Parser(RULE2)
            lst = []
            for match in parser.findall(add_text):
                lst.append((match.span, [_.value for _ in match.tokens]))
            if lst:
                dict_symp[feature] = 1
                dict_index[feature] = match.span
            else:
                dict_symp[feature] = 0
    
    GEO_RULE = morph_pipeline(['географический', 'выезжал'])
    GEO_RULE2 = rule(not_(normalized('не')),normalized('выезжал'))
    geo_space = [40,40]
    
    COND_RULE = morph_pipeline(['бытовые'])
    COND_RULE2 = rule(not_(normalized('не')),normalized('удовлетворительные'))
    cond_space = [0,60]
    SEC_COND_RULE = morph_pipeline(['Социально-бытовые'])
    sec_cond_space = [0,60]
    
    WORK_COND_RULE = morph_pipeline(['условия труда'])
    work_cond_space = [20,20]
    
    CONTACT_RULE = morph_pipeline(['контакт'])
    CONTACT_RULE2 = morph_pipeline(['да'])
    contact_space = [0,40]
    
    WATER_RULE = morph_pipeline(['сырой воды'])
    WATER_RULE2 = morph_pipeline(['не было', 'отрицает', 'нет'])
    water_space = [80,80]

    features = ['географический анамнез', 'бытовые условия', 'бытовые условия',
               'условия труда','контакт с зараженными','пищевой анамнез']
    rules = [GEO_RULE, COND_RULE, SEC_COND_RULE, WORK_COND_RULE,
            CONTACT_RULE, WATER_RULE]
    sec_rules = [GEO_RULE2, COND_RULE2, COND_RULE2, COND_RULE2,
            CONTACT_RULE2, WATER_RULE2]
    spaces = [geo_space, cond_space, sec_cond_space, work_cond_space,
             contact_space, water_space]
    
    for i in range(len(features)):
        find_feature(features[i],rules[i],sec_rules[i],spaces[i])

    # Rules for bad habbits
    HAB_RULE = morph_pipeline(['вредные привычки', 'алкоголь'])
    parser = Parser(HAB_RULE)
    hab_lst = []
    for match in parser.findall(text):
        hab_lst.append((match.span, [_.value for _ in match.tokens]))
    if hab_lst:
        dict_index['вредные привычки'] = match.span
        text_hab = text[match.span[1]-80:match.span[1]+80]
        HAB_RULE = morph_pipeline(['не было', 'отрицает', 'нет', 'не употребляет'])
        parser = Parser(HAB_RULE)
        hab_lst = []
        for match in parser.findall(text_hab):
            hab_lst.append((match.span, [_.value for _ in match.tokens]))
        if hab_lst:
            dict_symp['вредные привычки'] = 0
            dict_index['вредные привычки'] = match.span
        else:
            dict_symp['вредные привычки'] = 1

    SMOKE_RULE = or_(rule(not_(normalized('не')),normalized('курит')),
                     rule(not_(normalized('не')),normalized('употребляет')))
    parser = Parser(SMOKE_RULE)
    hab_lst = []
    for match in parser.findall(text):
        hab_lst.append((match.span, [_.value for _ in match.tokens]))
    if hab_lst:
        dict_symp['вредные привычки'] = 1
        dict_index['вредные привычки'] = match.span
    
    # Rules for work
    work_lst = []
    WORK_RULE = morph_pipeline(['работает'])
    parser = Parser(WORK_RULE)
    for match in parser.findall(text):
        work_lst.append((match.span, [_.value for _ in match.tokens]))
    if work_lst:
        dict_symp['соц категория'] = 0
        dict_index['соц категория'] = match.span

    WORK_RULE = rule(not_(normalized('не')),normalized('работает'))
    parser = Parser(WORK_RULE)
    work_lst = []
    for match in parser.findall(text):
        work_lst.append((match.span, [_.value for _ in match.tokens]))
    if work_lst:
        dict_symp['соц категория'] = 1
        dict_index['соц категория'] = match.span
    
    # If patient has условия труда probably he has a job
    if dict_symp['условия труда'] is not None:
        dict_symp['соц категория'] = 1
        
    # Rule for food
    FOOD_RULE = morph_pipeline(['питание'])
    parser = Parser(FOOD_RULE)
    food_lst = []
    for match in parser.findall(text):
        food_lst.append((match.span, [_.value for _ in match.tokens]))
    if food_lst:
        dict_index['избыточное питание'] = match.span
        text_food = text[match.span[1]-20:match.span[1]+20]
        FOOD_RULE = or_(rule(not_(normalized('не')),normalized('удовлетворительное')),
                        rule(not_(normalized('не')),normalized('полноценное')),
                        rule(not_(normalized('не')),normalized('домашнее')))
        parser = Parser(FOOD_RULE)
        food_lst = []
        for match in parser.findall(text_food):
            food_lst.append((match.span, [_.value for _ in match.tokens]))
        if food_lst:
            dict_symp['питание'] = 1
            dict_index['питание'] = match.span
        else:
            dict_symp['питание'] = 0

        FOOD_RULE = rule(not_(normalized('не')),normalized('избыточное'))
        parser = Parser(FOOD_RULE)
        food_lst = []
        for match in parser.findall(text_food):
            food_lst.append((match.span, [_.value for _ in match.tokens]))
        if food_lst:
            dict_index['избыточное питание'] = match.span
            dict_symp['избыточное питание'] = 1
        else:
            dict_symp['избыточное питание'] = 0
            
    # Rule for fish
    FISH_RULE = morph_pipeline(['рыба'])
    parser = Parser(FISH_RULE)
    fish_lst = []
    for match in parser.findall(text):
        fish_lst.append((match.span, [_.value for _ in match.tokens]))
    if fish_lst:
        dict_symp['речная рыба'] = 0
        dict_index['речная рыба'] = match.span
        text_fish = text[match.span[1]-40:match.span[1]+40]
        FISH_RULE = morph_pipeline(['да', 'постоянно'])
        parser = Parser(FISH_RULE)
        fish_lst = []
        for match in parser.findall(text_fish):
            fish_lst.append((match.span, [_.value for _ in match.tokens]))
        if fish_lst:
            dict_symp['речная рыба'] = 1
        FISH_RULE = rule(not_(normalized('не')),normalized('употребляет'))
        parser = Parser(FISH_RULE)
        fish_lst = []
        for match in parser.findall(text_fish):
            fish_lst.append((match.span, [_.value for _ in match.tokens]))
        if fish_lst:
            dict_symp['речная рыба'] = 1
            dict_index['речная рыба'] = match.span

    # Rule for home
    home = None
    home_span = None
    home_types = [['бездомный'],
                   ['дом благоустроенный', 'частный дом'],
                   ['дом не благоустроенный','дом неблагоустроенный'],
                   ['квартира не благоустроенная', 'квартира неблагоустроенная'],
                   ['квартира благоустроенная', 'благоустроенная квартира'],]

    for i in range(len(home_types)):
        home_lst = []
        HOME_RULE = morph_pipeline(home_types[i])
        parser = Parser(HOME_RULE)
        for match in parser.findall(text):
            home_lst.append((match.span, [_.value for _ in match.tokens]))
        if home_lst:
            home = i
            home_span = match.span

    dict_symp['квартира, дом'] = home
    dict_index['квартира, дом'] = home_span

    pets = []
    pets_span = []
    pet_types = [['кошка'],
                 ['собака'],
                 ['корова','коза']]

    # Rule for pets
    for i in range(len(pet_types)):
        pet_lst = []
        PET_RULE = morph_pipeline(pet_types[i])
        parser = Parser(PET_RULE)
        for match in parser.findall(text):
            pet_lst.append(' '.join([_.value for _ in match.tokens]))
            pets_span.append(match.span)
        if pet_lst:
            pets.append(i+1)

    if len(pets)>1:
        pets = 4
    elif pets:
        pets = pets[0]
    else:
        pets = 0
    dict_symp['домашние животные'] = pets
    dict_index['домашние животные'] = pets_span

    # Rules for different factors
    factors = []
    factors_span = []
    factor_types = [['ссадины',"царапины", "раны", "расчесы", "уколы", "потертости", "трещины", 'вскрытие', 'поцарапал', "рассечен"],
                   ['ушибы'],
                   ['переохлаждение','перегревание','смена температуры',"охлаждение"],
                   ['инсоляция'],
                   ['стресс', "стрессовая ситуация"],
                   ['переутомление', 'тяжело работал']]

    def find_factors(factor_types, text=text, left=0, right=len(factor_types)):
        for i in range(len(factor_types[left:right])):
            factor_lst = []
            FACT_RULE = morph_pipeline(factor_types[i+left])
            parser = Parser(FACT_RULE)
            for match in parser.findall(text):
                factor_lst.append(' '.join([_.value for _ in match.tokens]))
                factors_span.append(match.span)
            if factor_lst:
                factors.append(i+1+left)

    find_factors(factor_types)
    detect_lst = []
    parser = Parser(morph_pipeline(['трещин - не обнаружено']))
    for match in parser.findall(text):
        detect_lst.append(' '.join([_.value for _ in match.tokens]))
    if detect_lst:
        factors.remove(1)
        
    if factors:
        dict_symp['провоцирущие факторы'] = factors
        dict_index['провоцирущие факторы'] = factors_span
            
    factors = []
    factors_span = []
    factor_types = [['микоз',"диабет", "ожирение", "варикоз", "недостаточность", "лимфостаз", "экзема", "варикозная болезнь"],
                   ['тонзилит',"отит", "синусит", "кариес", "пародонтоз", "остеомиелит", "тромбофлебит", "трофические язвы"],
                   ['резиновая обувь','загрязнения кожных'],
                   ['соматические заболевания']]
    
    if dict_symp['др заболевания в анамнезе']:
        find_factors(factor_types, text=dict_symp['др заболевания в анамнезе'], right=2)
    find_factors(factor_types, left=2)
    if factors:
        dict_symp['предрасполагающие факторы'] = factors
        dict_index['предрасполагающие факторы'] = factors_span

    # Rule for detecting the second diagnosis
    DIAGNOZ_RULE = or_(rule(normalized('сопутствующий'), not_(or_(gram('NOUN')))),
                       rule(normalized('сопутствующий'),normalized('диагноз')),
                       rule(normalized('диагноз'),normalized('сопутствующий')),)

    parser = Parser(DIAGNOZ_RULE)
    diag_lst = []
    for match in parser.findall(text):
        diag_lst.append((match.span, [_.value for _ in match.tokens]))
    if diag_lst:
        dict_symp['сопутствующий диагноз'] = text[match.span[1]+2:match.span[1]+text[match.span[1]:].find(' \n  \n')]
        dict_index['сопутствующий диагноз'] = [match.span[1]+2,match.span[1]+text[match.span[1]:].find(' \n  \n')]
        dict_symp['кол-во сопут заболеваний'] = dict_symp['сопутствующий диагноз'].count('\n')
        if dict_symp['кол-во сопут заболеваний']==0: dict_symp['кол-во сопут заболеваний']=1

    # Rule for detecting the first diagnosis
    DIAGNOZ_RULE = or_(rule(normalized('диагноз'),normalized('при'),normalized('поступлении')),
                       rule(normalized('клинический'),normalized('диагноз')),
                       rule(normalized('диагноз'),normalized('клинический')),
                       rule(normalized('основной'),normalized('диагноз')),
                       rule(normalized('диагноз'),normalized('основной')),
                       rule(normalized('Ds')),
                       rule(normalized('Ds:')),
                       rule(not_(or_(gram('ADJF'),gram('NOUN'))),normalized('диагноз'),not_(or_(gram('ADJF'),gram('PREP')))))

    diag_lst = []
    parser = Parser(DIAGNOZ_RULE)
    for match in parser.findall(text):
        diag_lst.append((match.span, [_.value for _ in match.tokens]))
    last = match.span[1]+text[match.span[1]:].find(' \n  \n')
    if last == match.span[1]-1:
        last = len(text)-1
    dict_symp['основной диагноз'] = text[match.span[1]+1:last]
    dict_index['основной диагноз'] = [match.span[1]+1,last]

    # Rules for detecting ЛПТ and ППТ
    LEFT_RULE = morph_pipeline(['левая', 'слева'])
    parser = Parser(LEFT_RULE)
    side_lst = []
    for match in parser.findall(dict_symp['основной диагноз']):
        side_lst.append((match.span, [_.value for _ in match.tokens]))

    RIGHT_RULE = morph_pipeline(['правая', 'справа'])
    parser = Parser(RIGHT_RULE)
    for match in parser.findall(dict_symp['основной диагноз']):
        side_lst.append((match.span, [_.value for _ in match.tokens]))
    
    # If we dont have information about side in 'основной диагноз', check other diagnosis
    DIAGNOZ_RULE = or_(rule(normalized('Обоснование'),normalized('Диагноза')))
    part = dict_symp['основной диагноз']
    if len(side_lst) == 0:
        part = text[text.find('Диагноз'):]
        side_lst = []
        parser = Parser(DIAGNOZ_RULE)
        for match in parser.findall(part):
            side_lst.append((match.span, [_.value for _ in match.tokens]))
        last = match.span[1]+part[match.span[1]:].find(' \n  \n')
        if last == match.span[1]-1:
            last = len(part)-1
        explaining = part[match.span[1]+1:last]
        if len(explaining)>1:
            part = part.replace(explaining,' ')
    
    # If we dont have information about side in diagnosis, check other 'Жалобы'
    DIAGNOZ_RULE = or_(rule(normalized('Жалобы')))
    comp_lst = []
    parser = Parser(DIAGNOZ_RULE)
    for match in parser.findall(text):
        comp_lst.append((match.span, [_.value for _ in match.tokens]))
    last = comp_lst[0][0][1]+text[comp_lst[0][0][1]:].find(' \n  \n')
    if last == comp_lst[0][0][1]-1:
        last = len(text)-1
    zhalobi = text[comp_lst[0][0][1]+1:last]
    
    rozha_types = [['волосистая часть головы', 'волостистой части головы'], ['лицо','щека','лоб','глаз'],
                   ['нос','губы'],['верняя часть туловища', 'верхняя конечность'],['нижняя часть туловища'],
                   ['пах', 'половые органы'],['верняя часть спины'],['нижняя часть спины'],
                   ['плечо'],['предплечье'],['кисть'],['бедро'],['голень'],['стопа'],['голеностоп'], ["ушная раковина"]]
    
    def find_side(parser, sidetext):
        rozha = []
        lst = []
        for match in parser.findall(sidetext):
            lst.append((match.span, [_.value for _ in match.tokens]))
        if lst:
            for i in range(len(rozha_types)):
                rozha_lst = []
                TYPE = morph_pipeline(rozha_types[i])
                parser = Parser(TYPE)
                for match in parser.findall(sidetext):#part):
                    rozha_lst.append(' '.join([_.value for _ in match.tokens]))
                if rozha_lst:
                    if i ==15: rozha.append('2.1')
                    else: rozha.append(i+1)
        return(rozha)
    
    parser = Parser(LEFT_RULE)
    dict_symp['ЛПТ'] = find_side(parser, part)
    
    parser = Parser(RIGHT_RULE)
    dict_symp['ППТ'] = find_side(parser, part)
    
    if not dict_symp['ППТ'] and not dict_symp['ЛПТ']:
        parser = Parser(LEFT_RULE)
        dict_symp['ЛПТ'] = find_side(parser, zhalobi)
        
        parser = Parser(RIGHT_RULE)
        dict_symp['ППТ'] = find_side(parser, zhalobi)
        
    # Special rule for detecting face
    face_lst = []
    FACE_RULE = morph_pipeline(['нос','губы'])
    parser = Parser(FACE_RULE)
    for match in parser.findall(part):
        face_lst.append((match.span, [_.value for _ in match.tokens]))
    if face_lst:
        dict_symp['ППТ'].append(3)
        dict_symp['ЛПТ'].append(3)

    dict_symp['ЛПТ'] = list(set(dict_symp['ЛПТ']))
    dict_symp['ППТ'] = list(set(dict_symp['ППТ']))
    if not dict_symp['ППТ']: dict_symp['ППТ'] = None
    if not dict_symp['ЛПТ']: dict_symp['ЛПТ'] = None
        
    return dict_symp, dict_index