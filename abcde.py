
import googletrans
from googletrans import Translator

translator = Translator()

text_to_translate = translator.translate('Hola, cómo estás ?', src='en', dest= 'es')
print(text_to_translate)