from sqlalchemy import create_engine
from sqlalchemy import Column
from sqlalchemy import Integer, Text
from sqlalchemy import exists
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import csv
import os
import json


class Unit:
    all_units = {}

    def __init__(self, name=None):
        self.name = name
        self.__load_max = 0
        self.streams_in = dict()
        self.streams_out = dict()
        Unit.all_units[self.name] = self

    def set_load_max(self, value=0):
        self.__load_max = value

    def __repr__(self):
        return f"<{self.__class__.__name__}(name={self.name}," \
               f" __load_max={self.__load_max}," \
               f" streams_in={self.streams_in}," \
               f" streams_out={self.streams_out})>"


class AVTUnit(Unit):
    pass


class RerunningUnit(Unit):
    pass


class Stream:
    all_streams = {}

    def __init__(self, name=None):
        self.name = name
        self.dep_units = []
        self.dst_units = []
        Stream.all_streams[self.name] = self

    def __repr__(self):
        return f"<Stream(name={self.name}," \
               f" dep_units={self.dep_units}," \
               f" dst_units={self.dst_units})>"


__Base = declarative_base()


class __UnitTable(__Base):
    __tablename__ = 'unit'
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(Text)
    type = Column(Integer)

    def __repr__(self):
        return f"<Unit(id={self.id}, name={self.name}, type={self.type})>"


class __StreamTable(__Base):
    __tablename__ = 'stream'
    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(Text)

    def __repr__(self):
        return f"<Stream(id={self.id}, name={self.name})>"


class __UnitMaterialTable(__Base):
    __tablename__ = 'unit_material'
    unit_id = Column(Integer, primary_key=True)
    stream_id = Column(Integer, primary_key=True)
    feed_flag = Column(Integer)

    def __repr__(self):
        return f"<UnitMaterial(unit_id={self.unit_id}," \
               f" stream_id={self.stream_id}, feed_flag={self.feed_flag})>"


class __LoadMaxTable(__Base):
    __tablename__ = 'load_max'
    unit_id = Column(Integer, primary_key=True)
    value = Column(Integer)

    def __repr__(self):
        return f"<LoadMax(unit_id={self.unit_id}, value={self.value})>"


def get_units(session, verbose=False):
    new_unit = None
    query = session.query(__UnitTable.name, __UnitTable.type)
    for instance in query:
        if instance.type == 0:
            new_unit = AVTUnit(instance.name)
        elif instance.type == 1:
            new_unit = RerunningUnit(instance.name)
        if verbose:
            print(new_unit)


def set_load_max(session, verbose=False):
    query = session.query(__UnitTable.name, __LoadMaxTable.value)
    query = query.join(__LoadMaxTable,
                       __UnitTable.id == __LoadMaxTable.unit_id)
    for unit in Unit.all_units.values():
        load_max_value = query.filter(__UnitTable.name == unit.name).\
                                      first().value
        unit.set_load_max(load_max_value)
        if verbose:
            print(unit)


def set_related_streams(verbose=False):
    for stream in Stream.all_streams.values():
        for unit in stream.dep_units:
            Unit.all_units[unit].streams_out[stream.name] = stream
        for unit in stream.dst_units:
            Unit.all_units[unit].streams_in[stream.name] = stream
    if verbose:
        for unit in Unit.all_units.values():
            print(unit)


def get_streams(session, verbose=False):
    query = session.query(__StreamTable.name)
    for instance in query:
        new_stream = Stream(instance.name)
        if verbose:
            print(new_stream)


def set_related_units(session, verbose=False):
    for stream in Stream.all_streams.values():
        query = session.query(__StreamTable.name.label('stream_name'),
                              __UnitTable.name.label('unit_name'),
                              __UnitMaterialTable.feed_flag)
        query = query.join(__UnitMaterialTable,
                           __UnitMaterialTable.stream_id == __StreamTable.id)
        query = query.join(__UnitTable,
                           __UnitTable.id == __UnitMaterialTable.unit_id)
        query = query.filter(__StreamTable.name == stream.name)
        query = query.order_by('unit_name')
        for instance in query:
            if instance.feed_flag:
                stream.dst_units.append(instance.unit_name)
            else:
                stream.dep_units.append(instance.unit_name)
        if verbose:
            print(stream)


def task_3(session):
    query = session.query(__UnitTable.name.label('unit_name'),
                          __StreamTable.name.label('stream_name'))
    query = query.join(__UnitMaterialTable,
                       __UnitMaterialTable.unit_id == __UnitTable.id)
    query = query.join(__StreamTable,
                       __StreamTable.id == __UnitMaterialTable.stream_id)
    query = query.filter(__UnitMaterialTable.feed_flag == 1)
    query = query.order_by('unit_name')
    print()
    for instance in query:
        print(instance.unit_name, instance.stream_name)
    print('Task 3 - DONE')


def task_4(session, verbose=False):

    def write_to_csv(data, path=os.getcwd(), file='task_4.csv'):
        with open(''.join([path, '\\', file]), "w", newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=';')
            for line in data:
                writer.writerow(line)

    query = session.query(__StreamTable.id, __StreamTable.name)
    sub_query = ~exists().where(__UnitMaterialTable.stream_id ==
                                __StreamTable.id)
    query = query.filter(sub_query)
    write_to_csv(query.all())
    print()
    if verbose:
        print(query.all())
    print('Task 4 - DONE')


def task_5(session, verbose=False):
    
    def write_to_json(data, path=os.getcwd(), file='task_5.json'):
        with open(''.join([path, '\\', file]), "w") as json_file:
            json.dump(data, json_file)

    data = {}
    print()
    for stream in Stream.all_streams.values():
        if len(stream.dst_units) > 1:
            data[stream.name] = stream.dst_units
            if verbose:
                print(stream.name, stream.dst_units)
    write_to_json(data)
    print('Task 5 - DONE')


def task_6(session, verbose=False):
    pass


def main(dialect='sqlite:///', path='db.db'):
    engine = create_engine(''.join([dialect, path]), echo=False)
    Session = sessionmaker(bind=engine)
    with Session() as session:
        get_units(session)
        set_load_max(session)
        get_streams(session)
        set_related_units(session)
        set_related_streams(verbose=True)
        task_3(session)
        task_4(session, verbose=True)
        task_5(session, verbose=True)
        # task_6(session)


if __name__ == '__main__':
    main()
