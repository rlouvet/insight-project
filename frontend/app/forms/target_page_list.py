from flask_wtf import Form
from wtforms import SelectField, SubmitField

from wtforms import validators, ValidationError

class TargetPageListForm(Form):
   target_page = SelectField('Target page',choices = [(str(i), str(i)) for i in range(1,21)])
   submit = SubmitField("Send")
