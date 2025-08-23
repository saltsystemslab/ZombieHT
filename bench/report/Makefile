DOC=main
OPTS = -pdf
LATEX = pdflatex

default:
	@if (command -v latexmk > /dev/null) ; then latexmk $(OPTS) $(DOC) ; else make brute; fi 

cont: 
	latexmk -pvc $(OPTS) $(DOC)

brute:
	$(LATEX) $(DOC)
	bibtex $(DOC)
	$(LATEX) $(DOC)
	$(LATEX) $(DOC)

clean:
	latexmk -C