from flask import Flask, request, json, jsonify
from sqlalchemy import create_engine
from sqlalchemy.sql import text

app = Flask(__name__)

connects = 'postgresql://postgres:24434@localhost:5432/employeer'
engine = create_engine(connects, echo=False)

@app.route('/employee/total_leave', methods=['GET'])
def get_total_leave_employee():
    body_req = request.json
    start_period = body_req.get('start period')
    end_period = body_req.get('end period')
    with engine.connect() as connection:
        qry = text("SELECT employee.nik, employee.name, sum(date_part('day', leave.end_date-leave.start_date)+1) as total_cuti\
                    From employee JOIN leave ON employee.nik = leave.employee_nik\
                    Where leave.end_date <= :end_period AND leave.start_date >= :start_period\
                    Group by employee.nik, employee.name")
        result = connection.execute(qry, start_period=start_period, end_period=end_period)
        list_result =[]
        for value in result:
            list_result.append({"nik":value['nik'], "name":value['name'], "total cuti": int(value['total_cuti'])})
        return jsonify(list_result)

@app.route('/employee/add', methods=['POST'])
def add_employee():
    body_req = request.json
    nik = body_req.get('nik')
    name = body_req.get('name')
    start_year = body_req.get('start year')
    with engine.connect() as connection:
        qry = text("INSERT INTO public.employee(nik, name, start_year)\
                    VALUES (:nik, :name, :start_year)")
        result = connection.execute(qry, nik=nik, name=name, start_year=start_year)
        new_qry = text("SELECT * FROM public.employee")
        new_result = connection.execute(new_qry)
        post = []
        for value in new_result:
            post.append({"nik":value['nik'], "name":value['name'], "start year":value['start_year']})
        return jsonify(post)

@app.route('/employee/update', methods=['PUT'])
def update_employee():
    body_req = request.json
    nik = body_req.get('nik')
    name = body_req.get('name')
    start_year = body_req.get('start year')
    with engine.connect() as connection:
        qry = text("UPDATE public.employee SET name=:name, start_year=:start_year\
                    WHERE nik=:nik")
        result = connection.execute(qry, nik=nik, name=name, start_year=start_year)
        new_qry = text("SELECT * FROM public.employee")
        new_result = connection.execute(new_qry)
        put = []
        for value in new_result:
            put.append({"nik":value['nik'], "name":value['name'], "start year":value['start_year']})
        return jsonify(put)

@app.route('/employee/delete', methods=['DELETE'])
def delete_employee():
    body_req = request.json
    nik = request.args.get('nik')
    with engine.connect() as connection:
        qry = text("DELETE FROM public.employee\
                    WHERE nik=:nik")
        result = connection.execute(qry, nik=nik)
        new_qry = text("SELECT * FROM public.employee")
        new_result = connection.execute(new_qry)
        post = []
        for value in new_result:
            post.append({"nik":value['nik'], "name":value['name'], "start year":value['start_year']})
        return jsonify(post)

if __name__ == '__main__':
    app.run(debug=True)